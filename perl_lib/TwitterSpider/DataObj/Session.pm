package TwitterSpider::DataObj::Session;

use DateTime;
use DateTime::Format::DateParse;

use TwitterSpider::DataObj;
@ISA = ( 'TwitterSpider::DataObj' );

use strict;
use warnings;

sub load_latest
{
	my ($class, $spider) = @_;
	my $db = $spider->db;

	my $sql = "SELECT * FROM session ORDER BY id DESC LIMIT 1";

	my $sth = $db->query($sql); 
	if (!$sth->rows)
	{
		return undef;
	}

	my $session = $sth->fetchrow_hashref;

	return bless $db->obj_from_db($session), $class;
}

sub create
{
	my ($class, $spider) = @_;

	my $data = {
		start_time => DateTime->now->datetime,
		status => 'new'
	};

	my $self = bless $data, $class;

	$self->commit($spider);

	#reload from the database, so we have an ID
	my $session = TwitterSpider::DataObj::Session->load_latest($spider); 

	return $session;
}

sub expired
{
	my ($self, $spider) = @_;
	my $cfg = $spider->config;

	my $interval = $cfg->cfg('system','min_update_interval_hours') * 60; #minutes

	my $start_time = DateTime::Format::DateParse->parse_datetime($self->value('start_time'));

	my $delta_ms = DateTime->now->delta_ms($start_time);

	return 1 if ($delta_ms->delta_minutes >= $interval);
	return 0;
}


#non-oo call
sub mysql_tabledef
{
	return [
		'CREATE TABLE IF NOT EXISTS session (
			id INT NOT NULL AUTO_INCREMENT,
			start_time DATETIME,
			end_time DATETIME,
			status CHAR(10),
			PRIMARY KEY (id)
		)'
	];
}

#return the specied user in this session if it exists in the database
sub user
{
	my ($self, $spider, $user_id) = @_;

	my $id_bits =
	{
		'id' => $user_id,
		'session_id' => $self->value('id')
	};
	return TwitterSpider::DataObj::User->load($spider, $id_bits);
}

sub create_user
{
	my ($self, $spider, $user_id) = @_;

	return TwitterSpider::DataObj::User->create($spider, $user_id, $self);
}


sub class_id
{
	my ($self) = @_;
	return 'session';
}

sub id_fields
{
	my ($self) = @_;
	return [qw/ id /];
}

1;
