package TwitterSpider::TwitterInterface;

use strict;
use warnings;

use Net::Twitter::Lite::WithAPIv1_1;

sub new
{
	my ($class, $spider) = @_;

	my $data = {};

	$data->{spider} = $spider;

	$data->{api_map} = {
		search => 'search/tweets',
		lookup_users => 'users/lookup',
		friends_ids => 'friends/ids',
		followers_ids => 'followers/ids'
	};

	$data->{api_limits} = {
		'_fresh' => 0
	};

	$data->{twitter} = undef; #the actual interface.

	return bless $data, $class;
}

sub can_make_api_call
{
	my ($self, $api) = @_;

	if (!$self->{api_limits}->{_fresh})
	{
		$self->refresh_api_limits;
		$self->{api_limits}->{_fresh} = 30; #number of local decrements of any api before the whole thing is refreshed
	}
	$self->{api_limits}->{_fresh}--;
	if (!$self->{api_limits}->{$api})
	{
		return 0;
	}
	$self->{api_limits}->{$api}--;
	return 1;
}

sub refresh_api_limits
{
	my ($self) = @_;

	my $limits = $self->query('rate_limit_status');

	my $api_log_string = 'Last API Check: ';
	foreach my $api (values %{$self->{api_map}})
	{
		my ($type, $operation) = split(/\//, $api);

		my $remaining = $limits->{resources}->{$type}->{"/$type/$operation"}->{remaining};

		$self->{api_limits}->{$api} = $remaining;
		$api_log_string .= "$api -> $remaining ; ";
	}
	$api_log_string =~ s/ ; $//; #tidy up string (hack hack hack)

	$self->{spider}->output_status($api_log_string);
}

sub query
{
	my ($self, $method, $args) = @_;

	$self->connect_to_twitter() if !$self->{twitter};

	if ($method ne 'rate_limit_status')
	{
		die "Unrecognised method: $method\n" unless $self->{api_map}->{$method};
		return (429,undef) unless $self->can_make_api_call($self->{api_map}->{$method});
	}
	else
	{
		return $self->{twitter}->rate_limit_status(); #rate_limit_status doesn't return ($status, $data)
	}

	my $data;
	eval {
		$data = $self->{twitter}->$method($args);
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
	my ($self) = @_;
	my $spider = $self->{spider};

	my $cfg = $spider->config;
	$spider->output_status('Connecting to twitter');

	my %nt_args = (
		consumer_key        => $cfg->secret('twitter_api_keys','consumer_key'),
		consumer_secret     => $cfg->secret('twitter_api_keys','consumer_secret'),
		access_token        => $cfg->secret('twitter_api_keys','access_token'),
		access_token_secret => $cfg->secret('twitter_api_keys','access_token_secret'),
		traits => [qw/API::RESTv1_1/]
	);

	my $twitter = Net::Twitter::Lite::WithAPIv1_1->new( %nt_args );

#handle this error properly?
	if (!$twitter->authorized)
	{
		$spider->output_status('Not authorized');
	}
	else
	{
		$self->{twitter} = $twitter;
	}
}

sub get_friends_or_followers
{
	#$f set to either friends or followers
	my ($self, $screen_name, $f) = @_;
	my $spider = $self->{spider};

	my $user_ids = [];
	my $r = undef; #to hold one page of results
	my $status;

	while (1)
	{
		$spider->output_status("Retrieving $f for $screen_name...");

		my $params = {
			screen_name => $screen_name,
			include_user_entities => 1,
		};
		$params->{cursor} = $r->{next_cursor} if $r;

		my $method = $f .'_ids';

		($status, $r) = $self->query($method, $params);
		return ($status, undef) unless $status == 200; #errors (and running out of API) pass upwards

		$spider->output_status(scalar @{$r->{ids}} . " $f IDs returned.  Cursor is " . $r->{next_cursor});

		push @{$user_ids}, @{$r->{ids}};
		last unless $r->{next_cursor}; #will be 0 on the last page
	}
	return (200, $user_ids);
}


sub tweet_search
{
	my ($self, $q) = @_;
	my $spider = $self->{spider};

	$spider->output_status("Tweet Search for '$q'...");
	my ($status, $data) = $self->query('search', { q => $q, count => 100, include_entities => 1 });
	return ($status, ($data->{statuses}?$data->{statuses}:undef));
}




1;

