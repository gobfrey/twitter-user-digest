package TwitterSpider::Config;

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


sub new
{
	my ($class, $config_file) = @_;

	my $data = {};

	$data->{config} = Config::IniFiles->new( -file => $config_file );
	my $secrets_file = $data->{config}->val('system','secrets');
	$data->{secrets} = Config::IniFiles->new( -file => $secrets_file );

	return bless $data, $class;
}

sub cfg
{
	my ($self, @cfg_path) = @_;

	return $self->{config}->val(@cfg_path);
}

sub secret
{
	my ($self, @cfg_path) = @_;

	return $self->{secrets}->val(@cfg_path);
}

sub users_to_harvest
{
	my ($self) = @_;

	my @user_groups = $self->{config}->GroupMembers('user');
	my $users = [];
	foreach my $user_group (@user_groups)
	{
		my ($group, $user) = split(/\s+/, $user_group);
		push @{$users}, $user if
		(
			$user
			&& $user =~ m/^[0-9]*$/ #only numeric
		);
	}
	return $users;
}

sub user_harvest_config
{
	my ($self, $user_id) = @_;

	my $user_harvest_config = 
	{
		followers => 0,
		followers_followers => 0,
		followers_friends => 0,
		followers_tweets_from => 0,
		followers_tweets_mentioning => 0,
		friends => 0,
		friends_followers => 0,
		friends_friends => 0,
		friends_tweets_from => 0,
		friends_tweets_mentioning => 0,
		tweets_from => 0,
		tweets_mentioning => 0,
	};

	foreach my $c (keys %{$user_harvest_config})
	{
		if ($self->cfg("user $user_id", $c))
		{
			$user_harvest_config->{$c} = 1;
		}
	}

	return $user_harvest_config;
}




1;
