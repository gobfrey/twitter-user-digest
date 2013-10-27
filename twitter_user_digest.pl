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
	harvest_count => {},
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

my $nt = connect_to_twitter();
die "Couldn't connect to twitter\n" unless $nt;

if ($username)
{
	harvest_single_user($target_dir, $username);
}
else
{
	harvest_from_config();
}

exit;

sub session_dir_expired
{
	my ($session_dir) = @_;

	#is it time to start another session?
	my $current_time = time;
	my @bits = split(/\//, $session_dir);
	my $dir_time = pop @bits;

	my $delta = $current_time - $dir_time;
	my $interval = cfg('system','min_update_interval_hours') * 60 * 60;

	if ($delta > $interval)
	{
		return 1;
	}
	else
	{
		return 0;
	}
}

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
		if (session_dir_expired($working_dir))
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
		my $users_file = $working_dir . '/screen_names_to_harvest';
		my $user_info_state = harvest_in_session_dir($working_dir, $users_file, 'screen_name');
		my $extras = { friends => 1, followers => 1, tweets => 1};
		my $extra_data_state = enrich_in_session_dir($working_dir, $extras, $users_file, 'screen_name');

		if ($user_info_state eq 'complete' && $extra_data_state eq 'complete')
		{
			$harvest_state = 'complete';
		}
	}

	if ($harvest_state eq 'complete')
	{
		initialise_spider_list($working_dir); #we could add a spider depth arg at this point
		$session_state = 'spidering';
	}

	my $spider_state = 'incomplete';
	if ($session_state eq 'spidering')
	{
		my $users_file = $working_dir . '/user_ids_to_spider';
		my $user_info_state = harvest_in_session_dir($working_dir, $users_file, 'user_id');
		my $extras = { friends => 1, followers => 1, tweets => 0};
		my $extra_data_state = enrich_in_session_dir($working_dir, $extras, $users_file, 'user_id');

		if ($user_info_state eq 'complete' && $extra_data_state eq 'complete')
		{
			$harvest_state = 'complete';
		}
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
			my $filename = "$dir/by_screen_name/$screen_name/$f.json";
			if (
				$user_params->{"spider_$f"}
				&& -e $filename
			)
			{
				my $names = json_file($filename);
				foreach my $n (@{$names})
				{
					$names_to_spider{$n} = 1;
				}
			}
		}
	}

	write_to_file($dir . '/user_ids_to_spider', join("\n", sort {lc($a) cmp lc($b)} keys %names_to_spider));
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

#takes a list of screen names or user IDs and populates a set of directories named by user ID
sub userlist_to_directories
{
	my ($nt, $base_path, $userlist, $userlist_type) = @_;

	my $users;

	if (scalar @{$userlist} > 100)
	{
		die "userlist_to_directories: More than 100 users in list\n";
	}

	eval {
		$users = $nt->lookup_users({$userlist_type => join(',',@{$userlist}), inlude_entities => 1});
	};
	if ($@)
	{
		print STDERR "$@\n";
		return undef;
	}

	my $json = JSON->new->allow_nonref;
	foreach my $user (@{$users})
	{
		my $p = path_from_parts([$base_path, "by_user_id", $user->{id}]);
		my $data = $json->pretty->encode($user);
		write_to_file("$p/userdata.json", $data);
		#symlink the username directory to the userid directory
		my $p2 = path_from_parts([$base_path, "by_screen_name"]);
		$p2 .= '/' . $user->{screen_name};

		if (!-e $p2)
		{
			output_status("symlinking $p to $p2");
			symlink($p, $p2) or die "$!\n";;
		}

		$LOG->{harvest_count}->{user_data}++;
	}

	return 1; #success
}



#file type needs to be 'screen_name' or 'user_id'
sub harvest_in_session_dir
{
	my ($session_dir, $users_file, $file_type) = @_;

	my $nt = connect_to_twitter();
	die "Couldn't connect to twitter\n" unless $nt;

	my @user_refs = file_to_array($users_file);

	while (1)
	{
		last if (!scalar @user_refs); #we've processed them all.

		my @one_hundred;
		#there's a max of 100 IDs that can be passed to twitter
		#exit when 100 refs or we run out of refs
		while (
			scalar @one_hundred < 100
			&& scalar @user_refs
		)
		{
			my $id = shift @user_refs;

			next if -e ("$session_dir/by_$file_type/$id"); #we've already harvested this one.
			push @one_hundred, $id;
		}

		my $success = userlist_to_directories($nt, $session_dir, \@one_hundred, $file_type);

		return 'incomplete' if !$success; #problem with the API, exit here
	}
	return 'complete';
}

#for every entry in the user file that has a directory, enrich if necessary
#to be run after user data has been downloaded
sub enrich_in_session_dir
{
	my ($session_dir, $bits_to_harvest, $users_file, $file_type) = @_;

	my $nt = connect_to_twitter();
	die "Couldn't connect to twitter\n" unless $nt;

	my @user_refs = file_to_array($users_file);
	my $json = JSON->new->allow_nonref;

	my $complete = 1;
	ENRICH_TYPE: foreach my $enrich_type (qw/ friends followers tweets /)
	{
		USER: foreach my $user (@user_refs)
		{
			my $user_dir = "$session_dir/by_$file_type/$user";
			my $filename = "$user_dir/$enrich_type.json";
			if (!-d $user_dir)
			{
				#there's no directory, the user info hasn't been downloaded yet
				$complete = 0;
				next;
			}
			next if -e $filename; #we've already done this

			my $user_obj = json_file($user_dir . '/userdata.json');
			if (!$user_obj)
			{
				$complete = 0;
				next;
			}

			$username = $user_obj->{screen_name};

			if ($user_obj->{protected})
			{
				output_status("$user is protected, we'll get no rich data");
				next USER;
			}

			output_status("Enriching $enrich_type for $username");

			if (!valid_screen_name($username))
			{
				print STDERR "non-alpha character in username $username, skipping\n";
				next;
			}

			my $data = get_user_data($nt, $username, $enrich_type);

			if ($data)
			{
				$LOG->{harvest_count}->{$enrich_type}++;
				my $json_data = $json->pretty->encode($data);

				write_to_file($filename, $json_data);
			}
			else
			{
				$complete = 0;
				next ENRICH_TYPE; #we're probably out of API for this type
			}

		}
	}

	if ($complete)
	{
		return 'complete';
	}
	return 'incomplete';
}


sub json_file
{
	my ($filename) = @_;

	return undef unless -e $filename;
	my $json = JSON->new->allow_nonref;

	my $json_data;
	{
		local $/; #Enable 'slurp' mode
		open my $fh, "<", $filename;
		binmode $fh, ":utf8";
		$json_data = <$fh>;
		close $fh;
	}
	my $data = $json->decode($json_data);
	return $data;
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
	if (!-e $dir . '/user_ids_to_spider')
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

sub get_friends_or_followers
{
	my ($nt, $username, $f) = @_;

	my $data = [];
	my $r = undef; #to hold one page of results

	while (1)
	{
		my $params = {
			screen_name => $username,
			include_user_entities => 1,
		};

		if ($r)
		{
			$params->{cursor} = $r->{next_cursor};
		}

		output_status("Retrieving $f for $username...");

		my $method = $f .'_ids';
		eval{
			$r = $nt->$method($params);
		};
		if ($@)
		{
			print STDERR "$@\n";
			return undef;
		}

		output_status(scalar @{$r->{ids}} . " $f IDs returned.  Cursor is " . $r->{next_cursor});

		push @{$data}, @{$r->{ids}};
		last unless $r->{next_cursor}; #will be 0 on the last page
	}
	return $data;
}

#todo: check harvest params for user
sub get_user_data
{
	my ($nt, $username, $data_class) = @_;

	output_status("Retrieving $data_class User Information for $username...");
	my $data;

	if ($data_class eq 'friends')
	{
		$data = get_friends_or_followers($nt, $username, 'friends');
		if (!$data)
		{
			return undef; #probably out of API, try again later.
		} 

	}
	elsif ($data_class eq 'followers')
	{
		$data = get_friends_or_followers($nt, $username, 'followers');
		if (!$data)
		{
			return undef; #probably out of API, try again later.
		} 
	}
	elsif ($data_class eq 'tweets')
	{
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
				$data->{$k} = $nt->search($params->{$k});
			};
			if ($@)
			{
				print STDERR "$@\n";
				return undef;
			}
		}
	}

	return $data;
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
		push @users, $user if ($user && valid_screen_name($user)); #only alpha and _ -
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

