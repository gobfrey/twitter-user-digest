#!/usr/bin/perl

use strict;
use warnings;

use JSON;
use Encode qw(encode);
use Net::Twitter::Lite::WithAPIv1_1;
use DateTime;
use Getopt::Long;
use Cwd;
use File::Copy;
use Config::IniFiles;

#we always want the largest page available for friends and followers, which is currently 200.
my $F_PAGE_SIZE = 200;
#don't even try to harvest if there are more than MAX friends or followers (this is one page less than the max that can be got through the API, just in case)
my $MAX_F = 200 * 14;

#we will be checking these API limits
my @APIS_USED = qw( 
search/tweets
users/lookup
friends/list
followers/list
);

my $CONFIG;
my $VERBOSE = 0;
my $username = undef;
my $config_file = undef;
my $target_dir = getcwd();

my $LOG = {
	harvest_count => 0,
	messages => [],
	api_status => ''
};

Getopt::Long::Configure("permute");
GetOptions(
        'verbose' => \$VERBOSE,
        'username=s' => \$username,
        'config=s' => \$config_file,
	'save-to=s' => \$target_dir,
);

if (!$config_file)
{
	my $msg = "twitter_user_digest.pl --config=<configfile> [--username=<screen_name>] [--save-to=<path>] [--verbose]\n";
	$msg .= "  * If username is defined, only that username will be harvested (without spidering) and will be stored in the current directory or the --save-to path if defined\n";
	die $msg;
}
load_config($config_file);

if ($username)
{
	harvest_single_user($target_dir, $username);
}
else
{
	harvest_from_config();
}

exit;

#this function will continue harvesting across api windows, using list of usernames that need harvesting
sub harvest_from_config
{
	check_paths();

	my $working_dir = latest_session_dir();

	push @{$LOG->{messages}}, "Working on $working_dir";

	my $session_state = session_dir_state($working_dir);

	output_status("working in $working_dir ($session_state)");

	if ($session_state eq 'complete')
	{
		#is it time to start another session?
		my $current_time = time;
		my @bits = split(/\//, $working_dir);
		my $dir_time = pop @bits;

		my $delta = $current_time - $dir_time;
		my $interval = cfg('system','min_update_interval_hours') * 60 * 60;

		if ($delta > $interval)
		{
			output_status('Update interval exceeded, starting new harvest session');
			$working_dir = create_session_dir();
			$session_state = 'empty';
		}
		else
		{
			output_status('No need to harvest yet, most recent harvest within the update interval');
			push @{$LOG->{messages}}, 'Nothing to do';
			return;
		}

	}

	if ($session_state eq 'empty')
	{
		initialise_session_dir($working_dir);
		$session_state = 'harvesting';
	}

	my $harvest_state = 'incomplete';
	if ($session_state eq 'harvesting')
	{
		my @screen_names = file_to_array($working_dir . '/screen_names_to_harvest');
		$harvest_state = harvest_in_session_dir($working_dir, @screen_names);
	}

	if ($harvest_state eq 'complete')
	{
		initialise_spider_list($working_dir);
		$session_state = 'spidering';
	}

	my $spider_state = 'incomplete';
	if ($session_state eq 'spidering')
	{
		my @screen_names = file_to_array($working_dir . '/screen_names_to_spider');
		$spider_state = harvest_in_session_dir($working_dir, @screen_names);
	}

	if ($spider_state eq 'complete')
	{
		write_to_file($working_dir . '/completion_timestamp', time);
		$session_state = 'complete';
		create_by_users($working_dir); #create human browsable structure
	}
	push @{$LOG->{messages}}, "Got " . $LOG->{harvest_count} . " users' data.  Final State: $session_state";
	output_log();
}

sub create_by_users
{
	my ($dir) = @_;

	output_status("Creating by users view");

	my @parts = split(/\//, $dir);
	my $timestamp = $parts[$#parts];

	my @screen_names = file_to_array($dir . '/screen_names_to_harvest');

	foreach my $sn (@screen_names)
	{
		my $src_dir = "$dir/$sn";
		next unless -e $src_dir;

		my $target_dir = path_from_parts([by_user_path(), $sn, DateTime->from_epoch(epoch=>$timestamp)->datetime]);

		copy_user_files($src_dir, $target_dir);

		my $params = harvest_params($sn);
		foreach my $f (qw/ friends followers /)
		{
			if (
				$params->{"spider_$f"}
				&& -e "$src_dir/$f"
			)
			{
				my @fs = file_to_array("$src_dir/$f");
				foreach my $f_sn (@fs)
				{
					my $f_src_dir = "$dir/$f_sn";
					next unless -e $f_src_dir;

					my $f_target_dir = path_from_parts([$target_dir, $f . '_spidered', $f_sn]);
					
					copy_user_files($f_src_dir, $f_target_dir);
				}
			}
		}
	}
}

sub copy_user_files
{
	my ($src_dir, $target_dir) = @_;

	#copy files for this user
	foreach my $file(qw/ friends followers full_data.json /)
	{
		if (-e "$src_dir/$file")
		{
			copy("$src_dir/$file","$target_dir/$file");
		}
	}

}


sub initialise_spider_list
{
	my ($dir) = @_;

	my @harvest_names = file_to_array($dir . '/screen_names_to_harvest');

	my %names_to_spider;
	foreach my $screen_name (@harvest_names)
	{
		my $user_params = harvest_params($screen_name);

		foreach my $f (qw/ followers friends /)
		{
			if (
				$user_params->{"spider_$f"}
				&& -e "$dir/$screen_name/$f"
			)
			{
				my @names = file_to_array("$dir/$screen_name/$f");
				foreach my $n (@names)
				{
					$names_to_spider{$n} = 1;
				}
			}
		}
	}

	write_to_file($dir . '/screen_names_to_spider', join("\n", sort {lc($a) cmp lc($b)} keys %names_to_spider))
}

sub clone
{
	my ($h) = @_;

	my $h2;

	foreach my $k (keys %{$h})
	{
		$h2->{$k} = $h->{$k};
	}
	return $h2;
}


sub harvest_in_session_dir
{
	my ($session_dir, @screen_names) = @_;

	my $nt = connect_to_twitter();
	die "Couldn't connect to twitter\n" unless $nt;

	my $complete = 1;
	foreach my $username (@screen_names)
	{
		if (!valid_screen_name($username))
		{
			print STDERR "non-alpha character in username $username, skipping\n";
			next;
		}

		if (-e $session_dir . '/' . $username)
		{
			next; #we've already harvested this if it has a directory
		}

		my $user_data = get_user_data($nt, $username);

		if ($user_data)
		{
			$LOG->{harvest_count}++;
			my $path_parts = [$session_dir, $username];
			store_data($username, $user_data, $path_parts);
		}
		else
		{
			$complete = 0; #it's not complete because at least one user directory doesn't exist
			last; #just exit if we failed, we've probably run out of API, pick things up on the next run thorough
		}
	}

	if ($complete)
	{
		return 'complete';
	}
	return 'incomplete';
}


#one line per array element, chomped.
sub file_to_array
{
	my ($filename) = @_;

	my @arr;

	open FILE, $filename or die "Couldn't open $filename for reading\n";
	binmode FILE, ":utf8";

	while (<FILE>)
	{
		chomp;
		push @arr, $_;
	}
	close FILE;

	return @arr;
}


sub harvest_single_user
{
	my ($target_dir, $username) = @_;

	die "$target_dir is not a directory\n" if (!-d $target_dir);
	die "$username is not a valid username" if !valid_screen_name($username);

	my $nt = connect_to_twitter();
	die "Couldn't connect to twitter\n" unless $nt;

	my $user_data = get_user_data($nt, $username);

	if ($user_data)
	{
		my $path_parts = [$target_dir, $username, DateTime->now->datetime];
		store_data($username, $user_data, $path_parts);
	}
	else
	{
		print STDERR "Problems with $username: no data being stored\n";
	}
}

sub initialise_session_dir
{
	my ($dir) = @_;

	output_status("Initialising $dir");

	my @screen_names = screen_names_from_config();

	write_to_file($dir . '/screen_names_to_harvest', join("\n", sort {lc($a) cmp lc($b)} @screen_names));
}

sub write_to_file
{
	my ($filename, $str) = @_;

	output_status("writing to $filename");

	open FILE, ">$filename" or die "Couldn't open $filename for writing\n";
	binmode FILE, ":utf8";
	print FILE $str;
	close FILE;
}

sub session_dir_state
{
	my ($dir) = @_;

	if (!-e $dir . '/screen_names_to_harvest')
	{
		return 'empty';
	}
	if (!-e $dir . '/screen_names_to_spider')
	{
		return 'harvesting';
	}
	if (!-e $dir . '/completion_timestamp')
	{
		return 'spidering';
	}
	return 'complete';
}

sub latest_session_dir
{
	my $session_dir = by_session_path();

	opendir my($dh), $session_dir or die "Couldn't open dir '$session_dir': $!";
	my @files = grep { /^[0-9]+$/ } readdir $dh; #session directory is a numeric timestamp
	closedir $dh;

	if (!scalar @files)
	{
		return create_session_dir();
	}

	@files = sort {$b <=> $a} @files;

	return $session_dir . '/' . $files[0]; #return highest numeric timestamp -- latest dir	
}

sub create_session_dir
{
	my $session_parent = by_session_path();

	my $path_parts = [$session_parent, time];

	return path_from_parts($path_parts);
}


exit;

	my $nt = connect_to_twitter();

my @usernames;



if ($VERBOSE)
{
	#output at the end of the run if we're being verbose
	api_limits($nt);
}

sub output_status
{
        my (@message) = @_;

        return unless $VERBOSE;

        my $message = join('', @message);
        $message =~ s/\n/\n\t/g; #indent multiple lines

        print STDERR scalar localtime time,' -- ', $message, "\n";
}

sub output_log
{
	my $filename = cfg('system','log_file');

	open FILE, ">>$filename" or die "Couldn't open $filename for writing\n";

	print FILE "\n--------------------------------------------\n";
	print FILE "Run completed at ", DateTime->now->datetime, "\n";
	print FILE $LOG->{api_status}, "\n";
	foreach my $msg (@{$LOG->{messages}})
	{
		print FILE "$msg\n";
	}
	print FILE "--------------------------------------------\n";

}



sub store_data
{
	my ($username, $user_data, $path_parts) = @_;

	#check/create that the directory is there
	my $target_dir = path_from_parts($path_parts);
	output_status("Writing data to $target_dir");

	my $json = JSON->new->allow_nonref;
	my $json_data = $json->pretty->encode($user_data);

	my $filename = $target_dir . '/' . 'full_data.json';
	write_to_file($filename, $json_data);

	foreach my $p_type (qw/ friends followers /)
	{
		next unless $user_data->{$p_type} && ref($user_data->{$p_type});

		my @people;
		foreach my $p (@{$user_data->{$p_type}})
		{
			push @people, $p->{screen_name}; 
		}
		my $filename = $target_dir . '/' . $p_type;
		write_to_file($filename, join("\n", sort {lc($a) cmp lc($b)} @people));
	}

}

#creates if needed
sub target_dir
{
	my ($username) = @_;

}

sub get_friends_or_followers
{
	my ($nt, $username, $user, $f) = @_;

	#check API limits for followers and friends
	my $count = $user->[0]->{$f . "_count"};

	if ($count > $MAX_F)
	{
		return "Too many $f to harvest ($f)";
	}

	if ($count / $F_PAGE_SIZE > api_limit($nt, "$f/list"))
	{
		output_status("Giving up harvesting $username -- out of API for $f");
		return undef; #give up for now, we'll try again on the next run 
	}

	my $data = [];
	my $r = undef; #to hold one page of results
	while (1)
	{
		my $params = {
			screen_name => $username,
			include_user_entities => 1,
			count => $F_PAGE_SIZE
		};

		if ($r)
		{
			$params->{cursor} = $r->{next_cursor};
		}

		output_status("Retrieving $f for $username...");

		eval{
			$r = $nt->$f($params);
		};
		if ($@)
		{
			print STDERR "$@\n";
			return undef;
		}

		output_status(scalar @{$r->{users}} . " users returned.  Cursor is " . $r->{next_cursor});

		push @{$data}, @{$r->{users}};
		last unless $r->{next_cursor}; #will be 0 on the last page
	}
	return $data;
}

#todo: check harvest params for user
sub get_user_data
{
	my ($nt, $username) = @_;

	if (!api_limits($nt))
	{
		return undef;
	}
	my $userdata = {};

	output_status("Retrieving User Information for $username...");

	eval {
		$userdata->{user} = $nt->lookup_users({screen_name => $username, inlude_entities => 1});
	};
	if ($@)
	{
		print STDERR "$@\n";
		return undef;
	}

	if ($userdata->{user}->[0]->{protected})
	{
		output_status("$username is protected, we'll get no rich data");
		#we'll get no more data from this one
		return $userdata;
	}

	foreach my $f (qw/ friends followers /)
	{
		$userdata->{$f} = get_friends_or_followers($nt, $username, $userdata->{user}, $f);
		if (!$userdata->{$f})
		{
			return undef; #probably out of API, try again later.
		} 
	}

	#collect last page of tweet from and mentioning the user
	my $params =
	{
		from => {
			include_entities => 1,
			q => "from:$username",
			count => 100,
		},
		mentions => {
			include_entities => 1,
			q => "\@$username",
			count => 100,
		}
	};

	foreach my $k (keys %{$params})
	{
		output_status("Retrieving $k tweets for $username...");
		eval{
			$userdata->{tweets}->{$k} = $nt->search($params->{$k});
		};
		if ($@)
		{
			print STDERR "$@\n";
			return undef;
		}
	}

	output_status("Successfully Retrieved data for $username");

	return $userdata;
}


sub connect_to_twitter
{
	output_status('Connecting to twitter');

	my $key_file = cfg('system', 'api_keys_file');
	my $keys = Config::IniFiles->new(-file => $key_file);

	my %nt_args = (
		consumer_key        => $keys->val('twitter_api_keys','consumer_key'),
		consumer_secret     => $keys->val('twitter_api_keys','consumer_secret'),
		access_token        => $keys->val('twitter_api_keys','access_token'),
		access_token_secret => $keys->val('twitter_api_keys','access_token_secret'),
		traits => [qw/API::RESTv1_1/]
	);

	my $nt = Net::Twitter::Lite::WithAPIv1_1->new( %nt_args );

#handle this error properly?
	if (!$nt->authorized)
	{
		output_status('Not authorized');
		return undef;
	}
	return $nt;
}

sub api_limit
{
	my ($nt, $api) = @_;

	my $limits = $nt->rate_limit_status;

	my ($type, $operation) = split(/\//, $api);

	return $limits->{resources}->{$type}->{"/$type/$operation"}->{remaining};
};


sub api_limits
{
	my ($nt) = @_;

	my $limits = $nt->rate_limit_status;
	my $empty = 0;
	my $api_log_string = 'Last API Check: ';
	foreach my $api (@APIS_USED)
	{
		my ($type, $operation) = split(/\//, $api);

		my $remaining = $limits->{resources}->{$type}->{"/$type/$operation"}->{remaining};

		$empty = 1 if !$remaining;
		$api_log_string .= "$api -> $remaining ; ";
		output_status("$api -> $remaining (reset at: " . scalar(localtime($limits->{resources}->{$type}->{"/$type/$operation"}->{reset})) . ")");
	}

	$LOG->{api_status} = $api_log_string;

	if ($empty)
	{
		return 0;
	}
	return 1;
}

sub load_config
{
	my ($filename) = @_;

	$CONFIG = Config::IniFiles->new( -file => $filename );
}

sub cfg
{
	my (@cfg_path) = @_;

	return $CONFIG->val(@cfg_path);
}

sub screen_names_from_config
{
	my @user_groups = $CONFIG->GroupMembers('user');
	my @users;
	foreach my $user_group (@user_groups)
	{
		my ($group, $user) = split(/\s+/, $user_group);
		push @users, $user if $user;
	}
	return @users;
}

sub harvest_params
{
	my ($screen_name) = @_;

	my $harvest_params = 
	{
		spider_friends => 0,
		spider_followers => 0
	};

	foreach my $c (keys %{$harvest_params})
	{
		if (cfg("user $screen_name", $c))
		{
			$harvest_params->{$c} = 1;
		}
	}

	return $harvest_params;
}



sub check_paths
{
	my $path = cfg('system','storage_path');

	foreach my $p ($path, by_user_path(), by_session_path())
	{
		path_from_parts([$p]);
	}
}

#takes a path and a series of subdirectories under it and creates if necessary,
#return the full path to that dir
sub path_from_parts
{
	my ($path_parts) = @_;

	if (!scalar @{$path_parts})
	{
		die "Empty path parts -- error:\n"
	}

	my $path = '';

	while (1)
	{
		$path .= shift(@{$path_parts}) . '/';
		if (!-d $path)
		{
			mkdir $path || die "couldn't create $path\n";
		}
		last if !scalar @{$path_parts}; #exit loop if we've built all the path parts
	}
	chop $path; #remove trailing /  (hack, hack, hack)

	return $path;
}


sub by_user_path
{
	return cfg('system','storage_path') . '/by_user';
}

sub by_session_path
{
	return cfg('system','storage_path') . '/by_session';
}


sub valid_screen_name
{
	my ($username) = @_;

	#basic check -- this is user submitted data (probably)
	if ($username =~ m/[^A-Za-z0-9_-]/)
	{
		return 0;
	}
	return 1;
}

