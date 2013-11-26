#!/usr/bin/perl

use strict;
use warnings;

use JSON;
use Encode qw(encode);
use Net::Twitter::Lite::WithAPIv1_1;
use Getopt::Long;
use Cwd;
use Config::IniFiles;

#CONFIG is global, but accessed through the cfg fucntion
my $CONFIG;
#VERBOSE is global, but only checked in the output_status function
my $VERBOSE = 0;
#TWITTER is global, but accessed through the query_twitter method
my $TWITTER;

my $config_file = undef;
my $screen_name = undef;

Getopt::Long::Configure("permute");
GetOptions(
        'verbose' => \$VERBOSE,
        'config=s' => \$config_file,
        'screenname=s' => \$screen_name,
);

if (!$config_file || !$screen_name)
{
	my $msg = "get_userid.pl --screenname=<screen name> --config=<configfile> [--verbose]\n";
	die $msg;
}

load_config($config_file);

connect_to_twitter();

my $data;
eval {
	$data = $TWITTER->lookup_users({screen_name => [$screen_name]});
};

if ( $@ ) {
	print STDERR "$@\n";
	die;
}

print $data->[0]->{id}, "\n";

exit;

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

sub connect_to_twitter
{
	my $key_file = cfg('system', 'secrets');
	my $keys = Config::IniFiles->new(-file => $key_file);

	my %nt_args = (
		consumer_key        => $keys->val('twitter_api_keys','consumer_key'),
		consumer_secret     => $keys->val('twitter_api_keys','consumer_secret'),
		access_token        => $keys->val('twitter_api_keys','access_token'),
		access_token_secret => $keys->val('twitter_api_keys','access_token_secret'),
		traits => [qw/API::RESTv1_1/]
	);

	$TWITTER = Net::Twitter::Lite::WithAPIv1_1->new( %nt_args );

#handle this error properly?
	if (!$TWITTER->authorized)
	{
		print STDERR 'Not authorized';
		die;
	}
}


