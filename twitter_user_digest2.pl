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
	$session = $ts->create_new_session;
}

my $ids_to_harvest = $ts->config->users_to_harvest;

if ($session->value('status') eq 'complete')
{
	if ($session->expired)
	{
		$ts->output_status('Previous harvest completed and interval exceeded, starting new session');
		$session = ts->create_new_session();
	}
	else
	{
		$ts->output_status('No need to harvest yet, most recent harvest within the update interval');
		push @{$LOG->{messages}}, 'Nothing to do';
		return;
	}
}

#create user entries in the table
if ($session->{status} eq 'new')
{
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

exit;





