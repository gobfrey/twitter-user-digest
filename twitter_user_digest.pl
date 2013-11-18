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
use DBI;

#we will be checking these API limits
#this maps the Net::Twitter method to its API call
my %API_MAP = ( 
search => 'search/tweets',
lookup_users => 'users/lookup',
friends_ids => 'friends/ids',
followers_ids => 'followers/ids'
);

#CONFIG and SECRETS are global, but accessed through the cfg and secret fucntion
my $CONFIG;
my $SECRETS;
#DB is global, but accessed through the database layer fucntions
my $DB;
#VERBOSE is global, but only checked in the output_status function
my $VERBOSE = 0;
#API_LIMITS is global, but only modified by the initialise_api_limits and the can_make_api_call functions
my $API_LIMITS = {};
#TWITTER is global, but accessed through the query_twitter method
my $TWITTER;

my $config_file = undef;

my $LOG = {
	harvest_count => {},
	messages => [],
	api_status => ''
};

Getopt::Long::Configure("permute");
GetOptions(
        'verbose' => \$VERBOSE,
        'config=s' => \$config_file,
);

if (!$config_file)
{
	my $msg = "twitter_user_digest.pl --config=<configfile> [--verbose]\n";
	die $msg;
}

load_config($config_file);

initialise_db();

output_status('Starting Havesting');
harvest_from_config();

output_log();
output_status('Harvesting Complete');
exit;



#this function will continue harvesting across api windows, using list of usernames that need harvesting
sub harvest_from_config
{
	my $session = latest_session();

	use Data::Dumper;
	print STDERR Dumper $session;

	if (!$session)
	{
		$session = {
			start_time => DateTime->now->datetime,
			status => 'new'
		};
		db_write('session', $session);
		$session = latest_session(); #we've loaded from the database, so we have an ID now.
	}


	exit;

	my $working_dir = '';
	my $session_state = session_dir_state($working_dir);

	push @{$LOG->{messages}}, "Working on $working_dir";
	output_status("working in $working_dir ($session_state)");

	#if we're complete, create a new session dir if it's time
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
		my $extras = { friends => 1, followers => 1, tweets_from => 1, tweets_mentioning => 1};

		$harvest_state = harvest_from_users_file($working_dir, $working_dir . '/screen_names_to_harvest', $extras);
	}

	if ($harvest_state eq 'complete')
	{
		initialise_spider_list($working_dir); #we could add a spider depth arg at this point
		$session_state = 'spidering';
	}

	my $spider_state = 'incomplete';
	if ($session_state eq 'spidering')
	{
		my $extras = { friends => 1, followers => 1, tweets_from => 1, tweets_mentioning => 1 };
		$spider_state = harvest_from_users_file($working_dir, $working_dir . '/user_ids_to_spider', 'user_id', $extras);
	}

	if ($spider_state eq 'complete')
	{
		write_to_file($working_dir . '/completion_timestamp', time);
		$session_state = 'complete';
#now out of date and needs rewriting -- check if there's a requirement
#		create_by_users($working_dir); #create human browsable structure
	}
	push @{$LOG->{messages}}, "Final State: $session_state";
}

sub harvest_from_users_file
{
	my ($working_dir, $users_file, $bits_to_harvest) = @_;

	my $user_info_state = get_basic_user_data($working_dir, $users_file);
	my $extra_data_state = enrich_in_session_dir($working_dir, $bits_to_harvest, $users_file);

	if ($user_info_state eq 'complete' && $extra_data_state eq 'complete')
	{
		return 'complete';
	}
	return 'incomplete';
}


#file type needs to be 'screen_name' or 'user_id'
sub get_basic_user_data
{
	my ($session_dir, $users_file, $file_type) = @_;

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

		if (scalar @one_hundred)
		{
			my $status = userlist_to_directories($session_dir, \@one_hundred, $file_type);
			
			return 'incomplete' if $status != 200; #problem with the API, exit here
		}
	}
	return 'complete';
}




#for every entry in the user file that has a directory, enrich if necessary
#to be run after user data has been downloaded
sub enrich_in_session_dir
{
	my ($session_dir, $bits_to_harvest, $users_file, $file_type) = @_;

	my @user_refs = file_to_array($users_file);
	my $json = JSON->new->allow_nonref;

	my $complete = 1;
	ENRICH_TYPE: foreach my $enrich_type (keys %{$bits_to_harvest})
	{
		next unless $bits_to_harvest->{$enrich_type};
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

			my $username = $user_obj->{screen_name};

			if ($user_obj->{protected})
			{
				output_status("$user is protected, we'll get no rich data");
				next USER;
			}

			output_status("Enriching $enrich_type for $username");

			my ($status, $data);
			if (
				($enrich_type eq 'frields' || $enrich_type eq 'followers')
				&& $user_obj->{$enrich_type . '_count'} > (15 * 5000) #the max accessible in a single window
			)
			{
				$data = [];
				$status = 200; #we'll pretend this is OK, but really it's unachievable
			}
			else
			{
				($status, $data) = get_user_data($username, $enrich_type);
			}

			if ($status == 200) #all OK
			{
				$LOG->{harvest_count}->{$enrich_type}++;
				my $json_data = $json->pretty->encode($data);

				write_to_file($filename, $json_data);
			}
			elsif ($status == 429) # Out of API
			{
				output_status("Out of API for $enrich_type");
				$complete = 0;
				next ENRICH_TYPE; #we're probably out of API for this type
			}
			elsif ($status >= 500 && $status < 600)
			{
				print STDERR "$status: terminating";
				last ENRICH_TYPE; #exit just to be safe
			}
			else #write empty data to the file.  It's probably a permissions error
			{
				output_status("HTTP STATUS $status for $enrich_type for $username.  Writing Empty Datafile.");
				write_to_file($filename, $json->pretty->encode([]));
			}	
		}
	}

	if ($complete)
	{
		return 'complete';
	}
	return 'incomplete';
}

















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
	my ($base_path, $userlist, $userlist_type) = @_;

	die "userlist_to_directories: More than 100 users in list\n" if scalar @{$userlist} > 100;

	my ($status, $users) = query_twitter('lookup_users', {$userlist_type => $userlist, include_entities => 1});
	return $status unless $status == 200; #probably out of API

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

	return $status; 
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

sub initialise_session_dir
{
	my ($dir) = @_;

	output_status("Initialising $dir");

	my @ids = ids_from_config();

	write_to_file($dir . '/ids_to_harvest', join("\n", sort {lc($a) cmp lc($b)} @ids));
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

	if (!-e $dir . '/ids_to_harvest')
	{
		return 'empty';
	}
	if (!-e $dir . '/ids_to_spider')
	{
		return 'harvesting';
	}
	if (!-e $dir . '/ids_to_hydrate')
	{
		return 'hydrating';
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

	refresh_api_limits();

	open FILE, ">>$filename" or die "Couldn't open $filename for writing\n";

	print FILE "\n--------------------------------------------\n";
	print FILE "Run completed at ", DateTime->now->datetime, "\n";
	print FILE $LOG->{api_status}, "\n";
	print FILE "Harvest Counts\n";
	foreach my $count_type (keys %{$LOG->{harvest_count}})
	{
		print FILE "\t $count_type : ", $LOG->{harvest_count}->{$count_type}, "\n";
	}
	foreach my $msg (@{$LOG->{messages}})
	{
		print FILE "$msg\n";
	}
	print FILE "--------------------------------------------\n";

}

sub get_friends_or_followers
{
	#$f set to either friends or followers
	my ($username, $f) = @_;

	my $user_ids = [];
	my $r = undef; #to hold one page of results
	my $status;

	while (1)
	{
		output_status("Retrieving $f for $username...");

		my $params = {
			screen_name => $username,
			include_user_entities => 1,
		};
		$params->{cursor} = $r->{next_cursor} if $r;

		my $method = $f .'_ids';

		($status, $r) = query_twitter($method, $params);
		return ($status, undef) unless $status == 200; #errors (and running out of API) pass upwards

		output_status(scalar @{$r->{ids}} . " $f IDs returned.  Cursor is " . $r->{next_cursor});

		push @{$user_ids}, @{$r->{ids}};
		last unless $r->{next_cursor}; #will be 0 on the last page
	}
	return (200, $user_ids);
}

#todo: check harvest params for user
sub get_user_data
{
	my ($username, $data_class) = @_;

	output_status("Retrieving $data_class User Information for $username...");
	my ($status, $data);

	if ($data_class eq 'friends' || $data_class eq 'followers')
	{
		($status, $data) = get_friends_or_followers($username, $data_class);
	}
	elsif ($data_class eq 'tweets_from')
	{
		($status, $data) = tweet_search("from:$username");
	}
	elsif ($data_class eq 'tweets_mentioning')
	{
		($status, $data) = tweet_search("\@$username");
	}
	else
	{
		die "Unrecognised Data Class $data_class\n";
	}

	return ($status, $data);
}

sub tweet_search
{
	my ($q) = @_;
	output_status("Tweet Search for '$q'...");
	my ($status, $data) = query_twitter('search', { q => $q, count => 100, include_entities => 1 });
	return ($status, $data->{statuses});
}

sub load_config
{
	my ($filename) = @_;

	$CONFIG = Config::IniFiles->new( -file => $filename );
}

sub load_secrets
{
	my ($filename) = @_;

	$SECRETS = Config::IniFiles->new( -file => $filename );
}


sub secret
{
	my (@secret_path) = @_;

	load_secrets(cfg('system','secrets')) unless $SECRETS;
	return $SECRETS->val(@secret_path);
}

sub cfg
{
	my (@cfg_path) = @_;

	return $CONFIG->val(@cfg_path);
}

sub ids_from_config
{
	my @user_groups = $CONFIG->GroupMembers('user');
	my @users;
	foreach my $user_group (@user_groups)
	{
		my ($group, $user) = split(/\s+/, $user_group);
		push @users, $user if ($user && valid_id($user)); #only numeric
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

	my $paths = [
		cfg('system','storage_path'),
#		by_user_path(),
		by_session_path()
	];

	foreach my $p (@{$paths})
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


sub valid_id
{
	my ($id) = @_;

	#basic check -- this is user submitted data (probably)
	if ($id =~ m/^[0-9]$/)
	{
		return 0;
	}
	return 1;
}


#############################################################
#
# Twitter Layer
#
#############################################################


sub can_make_api_call
{
	my ($api) = @_;

	if (!$API_LIMITS->{_fresh})
	{
		refresh_api_limits();
		$API_LIMITS->{_fresh} = 30; #number of local decrements of any api before the whole thing is refreshed
	}
	$API_LIMITS->{_fresh}--;
	if (!$API_LIMITS->{$api})
	{
		return 0;
	}
	$API_LIMITS->{$api}--;
	return 1;
}

sub refresh_api_limits
{
	my $limits = query_twitter('rate_limit_status');

	my $api_log_string = 'Last API Check: ';
	foreach my $api (values %API_MAP)
	{
		my ($type, $operation) = split(/\//, $api);

		my $remaining = $limits->{resources}->{$type}->{"/$type/$operation"}->{remaining};

		$API_LIMITS->{$api} = $remaining;
		$api_log_string .= "$api -> $remaining ; ";
	}
	$api_log_string =~ s/ ; $//; #tidy up string (hack hack hack)

	output_status($api_log_string);
	$LOG->{api_status} = $api_log_string;
}

sub query_twitter
{
	my ($method, $args) = @_;

	connect_to_twitter() if !$TWITTER;

	if ($method ne 'rate_limit_status')
	{
		die "Unrecognised method: $method\n" unless $API_MAP{$method};
		return (429,undef) unless can_make_api_call($API_MAP{$method});
	}
	else
	{
		return $TWITTER->rate_limit_status(); #rate_limit_status doesn't return ($status, $data)
	}

	my $data;
	eval {
		$data = $TWITTER->$method($args);
	};

	if ( my $err = $@ ) {
		if (!UNIVERSAL::can($err,'isa') || !$err->isa('Net::Twitter::Lite::Error'))
		{
			print STDERR "$@\n";
			return ( 500, undef ); #probably 503 -- service unavailable
		}
		return ($err->code, $data); #HTTP response code
	}

	return( 200,  $data );
}

sub connect_to_twitter
{
	output_status('Connecting to twitter');

	my %nt_args = (
		consumer_key        => secret('twitter_api_keys','consumer_key'),
		consumer_secret     => secret('twitter_api_keys','consumer_secret'),
		access_token        => secret('twitter_api_keys','access_token'),
		access_token_secret => secret('twitter_api_keys','access_token_secret'),
		traits => [qw/API::RESTv1_1/]
	);

	$TWITTER = Net::Twitter::Lite::WithAPIv1_1->new( %nt_args );

#handle this error properly?
	if (!$TWITTER->authorized)
	{
		output_status('Not authorized');
		return undef;
	}
}


###############################
#
# Database Layer
#
###############################


sub db_connect
{
	output_status('connecting to database');

	my $database = cfg('database','db_name');
	my $hostname = cfg('database','db_host');
	my $port = cfg('database','db_port');
	my $password = secret('database','db_password');
	my $user = secret('database','db_username');

	my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";

	$DB = DBI->connect($dsn, $user, $password)
                or die "Couldn't connect to database: " . DBI->errstr;

}

sub initialise_db
{
	my $queries = [
		'CREATE TABLE IF NOT EXISTS session (
			id INT NOT NULL AUTO_INCREMENT,
			start_time DATETIME,
			end_time DATETIME,
			status char(10),
			PRIMARY KEY (id)
		)',
		'CREATE TABLE IF NOT EXISTS user (
			session_id INT NOT NULL,
			id INT NOT NULL,
			json MEDIUMTEXT,
			PRIMARY KEY (session_id, id),
			FOREIGN KEY (session_id) REFERENCES session(id)
		)',
		'CREATE TABLE IF NOT EXISTS user_friends (
			session_id INT NOT NULL,
			user_id INT NOT NULL,
			friend_id INT NOT NULL,
			KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
		'CREATE TABLE IF NOT EXISTS user_followers (
			session_id INT NOT NULL,
			user_id INT NOT NULL,
			follower_id INT NOT NULL,
			KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
		'CREATE TABLE IF NOT EXISTS user_tweets_from (
			session_id INT NOT NULL,
			user_id INT NOT NULL,
			json_tweets LONGTEXT,
			PRIMARY KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
		'CREATE TABLE IF NOT EXISTS user_tweets_about (
			session_id INT NOT NULL,
			user_id INT NOT NULL,
			json_tweets LONGTEXT,
			PRIMARY KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
	];

	db_query($_) foreach @{$queries};

}

sub db_query
{
	my ($sql, @args) = @_;

	db_connect unless $DB; 

	output_status("Running $sql");

	my $sth = $DB->prepare($sql)
		or die "Couldn't prepare statement: " . $DB->errstr;

	$sth->execute(@args)
		or die "Couldn't execute statement: " . $sth->errstr;

	return $sth;
}

sub db_write
{
	my ($table_name, $hashref) = @_;

	my @colnames;
	my @values;
	my @questionmarks;

	foreach my $k (keys %{$hashref})
	{
		push @colnames, "`$k`";
		push @values, $hashref->{$k};
		push @questionmarks, '?';
	}

	my $sql = "INSERT INTO $table_name (" . join(', ',@colnames) . ') VALUES (' . join(', ',@questionmarks) . ')';
	db_query($sql, @values);
}

sub db_update
{
	my ($table_name, $hashref) = @_;

	die ("Cannot update with an id set") unless $hashref->{id};

	my @bits;
	my @values;
	foreach my $k (keys %{$hashref})
	{
		next if $k eq 'id';
		push @bits, "`$k`=?";
		push @values, $hashref->{$k};
	}

	my $sql = "UPDATE $table_name SET " . join(', ', @bits) . " WHERE `id`=?";
	push @values, $hashref->{id};

	db_query($sql, @values);
}

sub latest_session
{
	my $sql = "SELECT * FROM session ORDER BY ID DESC limit 1";

	my $sth = db_query($sql); 

	my $session = $sth->fetchrow_hashref;

	return $session;
}



