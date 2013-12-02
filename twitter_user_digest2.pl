#!/usr/bin/perl

use FindBin;
use lib "$FindBin::Bin/perl_lib";

use TwitterSpider;
use Getopt::Long;

use strict;
use warnings;

my $verbose = 0;
my $config_file = undef;

my $LOG = {
	harvest_count => {},
	messages => [],
	api_status => ''
};

Getopt::Long::Configure("permute");
GetOptions(
        'verbose' => \$verbose,
        'config=s' => \$config_file,
);

if (!$config_file)
{
	my $msg = "twitter_user_digest.pl --config=<configfile> [--verbose]\n";
	die $msg;
}

my $ts = TwitterSpider->new($config_file, $verbose);

$ts->output_status('Starting Process');

my $session = $ts->latest_session;
if (!$session)
{
	$ts->output_status('Creating new Session');
	$session = $ts->create_new_session;
}

$ts->output_status('Session has status: ' . $session->value('status'));

my $ids_to_harvest = $ts->config->users_to_harvest;

if ($session->value('status') eq 'complete')
{
	if ($session->expired($ts))
	{
		$ts->output_status('Previous harvest completed and interval exceeded, starting new session');
		$session = $ts->create_new_session();
	}
	else
	{
		$ts->output_status('No need to harvest yet, most recent harvest within the update interval');
		push @{$LOG->{messages}}, 'Nothing to do';
	}
}

#create user entries in the table
if ($session->{status} eq 'new')
{
	$ts->output_status('Initialising Session');
	foreach my $userid (@{$ids_to_harvest})
	{
		my $user = $session->user($ts, $userid);
		if (!$user)
		{
			$user = $session->create_user($ts,$userid);
			$user->set_value('harvest_root', 1);
			$user->commit($ts);
		}
	}
	$session->set_value('status','harvesting');
	$session->commit($ts);
}

my $harvest_statuses = {};
if ($session->value('status') eq 'harvesting')
{
	$ts->output_status('Downloading Data for Root Users');
	#download the user JSON -- a must for each user
	$harvest_statuses->{$session->download_user_data($ts)}++; #count the number of complete or incomplete statuses
	$harvest_statuses->{$session->download_user_extras($ts)}++;
}

if (
	$session->value('status') eq 'harvesting'
	&& !$harvest_statuses->{incomplete}
)
{
	$ts->output_status('Harvesting Successfully Completed');
	#create user objects from friends and followers, basing the friends, followers, tweets from and tweets about requirements on the 'parent' user's harvest parameters
	foreach my $userid (@{$ids_to_harvest})
	{
		my $user = $session->user($ts, $userid);
		if (!$user)
		{
			die "Couldn't load user $userid\n";
		}
		$user->create_children($ts);
	}

	$session->{status} = 'spidering';
	$session->commit($ts);
}

my $spider_statuses = {};
if ($session->{status} eq 'spidering')
{
	$ts->output_status('Downloading Frinds and Followers Data');
	#download the user JSON -- a must for each user
	$spider_statuses->{$session->download_user_data($ts)}++; #count the number of complete or incomplete statuses
	$spider_statuses->{$session->download_user_extras($ts)}++;
}

if (
	$session->{status} eq 'spidering'
	&& !$spider_statuses->{'incomplete'}
)
{
	$ts->output_status('Downloading Frinds and Followers Data Complete');
	#create terminal user records for all friends and followers
	$session->create_spider_children($ts);

	$session->set_value('status','terminating');
	$session->commit($ts);
}

my $terminating_status = 'incomplete';
if ($session->{status} eq 'terminating')
{
	$ts->output_status('Downloading final users data');
	#download the user JSON -- a must for each user
	$terminating_status = $session->download_user_data($ts);
}

if ($terminating_status eq 'complete')
{
	$ts->output_status('Downloading final users data Complete');
	$session->set_value('end_time',DateTime->now->datetime);
	$session->set_value('status','complete');
	$session->commit($ts);
}


$ts->output_status('Process finished.  Session has status: ' . $session->value('status'));

exit;





