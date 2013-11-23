package TwitterSpider::DataObj::User;

use TwitterSpider::DataObj;
@ISA = ( 'TwitterSpider::DataObj' );

use strict;
use warnings;


sub create
{
	my ($class, $spider, $id, $session) = @_;

	die "attempted to create user without session" unless $session;
	my $session_id = $session->id;
	$spider->output_status("Creating User $id for session $session_id");

	my $harvest_config = $spider->config->user_harvest_config($id);
	my $user = {
		'id' => $id,
		'session_id' => $session_id,
		'harvest_root' => 0,
		'harvest_config_json' => $harvest_config,
		'user_data_state' => 'TODO',
	};

	foreach my $c (qw/ followers friends tweets_from tweets_mentioning /)
	{
		my $col_name = $c . '_state';
		if ($harvest_config->{$c})
		{
			$user->{$col_name} = 'TODO';
		}
		else
		{
			$user->{$col_name} = 'NO';
		}
	}

	my $self = bless $user, $class;
	$self->commit($spider);

	$user = TwitterSpider::DataObj::User->load($spider, {id => $id, session_id => $session_id});
	return $user;
}

sub load_extra
{
	my ($self, $spider, $extra) = @_;
	my $db = $spider->db;
	my $session_id = $self->value('session_id');
	my $id = $self->value('id');

	if (
		$extra eq 'friends'
		|| $extra eq 'followers'
	)
	{
		my $sql = 'SELECT ' . $extra . "_id FROM user_$extra WHERE session_id = $session_id AND user_id = $id";
		my $sth = $db->query($sql);

		my $ids = [];
		while (my $f_id = $sth->fetchrow_arrayref)
		{
			push @{$ids}, $self->val_from_db($extra . '_id', $f_id->[0]);
		}

		return $ids;
	}

	if (
		$extra eq 'tweets_from'
		|| $extra eq 'tweets_mentioning'
	)
	{
		my $sql = "SELECT tweets_json FROM user_$extra WHERE session_id = $session_id AND user_id = $id";
		my $sth = $self->db_query($sql);

		my $tweets = $sth->fetchrow_arrayref;

		return $self->val_from_db('tweets_json', $tweets->[0]);
	}

	$spider->output_status("Request for unrecognised extra: $extra");
	return undef;


}

#non-oo call
sub mysql_tabledef
{
	return [
		'CREATE TABLE IF NOT EXISTS user (
			session_id INT NOT NULL,
			id BIGINT NOT NULL,
			harvest_root TINYINT,
			screen_name VARCHAR(255),
			user_data_json LONGTEXT,

			user_data_state CHAR(10),
			friends_state CHAR(10),
			followers_state CHAR(10),
			tweets_from_state CHAR(10),
			tweets_mentioning_state CHAR(10),

			harvest_config_json VARCHAR(255),

			PRIMARY KEY (session_id, id),
			KEY (session_id, harvest_root),
			KEY (session_id, user_data_state),
			KEY (session_id, friends_state),
			KEY (session_id, followers_state),
			KEY (session_id, tweets_from_state),
			KEY (session_id, tweets_mentioning_state),
			FOREIGN KEY (session_id) REFERENCES session(id)
		)',
		'CREATE TABLE IF NOT EXISTS user_friends (
			session_id INT NOT NULL,
			user_id BIGINT NOT NULL,
			friends_id BIGINT NOT NULL,
			KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id),
			PRIMARY KEY (session_id, user_id, friends_id)
		)',
		'CREATE TABLE IF NOT EXISTS user_followers (
			session_id INT NOT NULL,
			user_id BIGINT NOT NULL,
			followers_id BIGINT NOT NULL,
			KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id),
			PRIMARY KEY (session_id, user_id, followers_id)
		)',
		'CREATE TABLE IF NOT EXISTS user_tweets_from (
			session_id INT NOT NULL,
			user_id BIGINT NOT NULL,
			tweets_json LONGTEXT,
			PRIMARY KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
		'CREATE TABLE IF NOT EXISTS user_tweets_mentioning (
			session_id INT NOT NULL,
			user_id BIGINT NOT NULL,
			tweets_json LONGTEXT,
			PRIMARY KEY (session_id, user_id),
			FOREIGN KEY (session_id) REFERENCES session(id),
			FOREIGN KEY (session_id, user_id) REFERENCES user(session_id, id)
		)',
	];
}


sub class_id
{
	my ($self) = @_;
	return 'user';
}

sub id_fields
{
	my ($self) = @_;
	return [qw/ id session_id /];
}

1;
