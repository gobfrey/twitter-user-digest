package TwitterSpider;

use TwitterSpider::Config;
use TwitterSpider::DB;
use TwitterSpider::DataObj::User;
use TwitterSpider::DataObj::Session;
use TwitterSpider::TwitterInterface;
use TwitterSpider::PageContent;

sub new
{
	my ($class, $config_file, $session_id, $user_id, $verbose) = @_;

	die "Cannot create TwitterSpider without a config file\n" unless $config_file;
	die "$config_file doesn't exist\n" unless -e $config_file;

	#the current user and session for context throughout
	my $self = bless {
		user_id => $user_id,
		session_id => $session_id
	}, $class;
	
	my $config = TwitterSpider::Config->new($config_file);
	$self->{config} = $config;

	#these two must be done after the config is loaded
	$self->{db} = TwitterSpider::DB->new($self);
	$self->{twitter} = TwitterSpider::TwitterInterface->new($self);

	$self->{verbose} = $verbose;


	return $self;
}

sub user_id
{
	my ($self) = @_;
	return $self->{user_id};
}

sub session_id
{
	my ($self) = @_;
	return $self->{session_id};
}


sub twitter
{
	my ($self) = @_;

	return $self->{twitter};
}

sub db
{
	my ($self) = @_;
	return $self->{db};
}

sub config
{
	my ($self) = @_;
	return $self->{config};
}

sub output_status
{
        my ($self, @message) = @_;

        return unless $self->{verbose};

        my $message = join('', @message);
        $message =~ s/\n/\n\t/g; #indent multiple lines

        print STDERR scalar localtime time,' -- ', $message, "\n";
}

sub latest_session
{
	my ($self) = @_;

	return TwitterSpider::DataObj::Session->load_latest($self);
}

sub create_new_session
{
	my ($self) = @_;

	return TwitterSpider::DataObj::Session->create($self);
}

1;
