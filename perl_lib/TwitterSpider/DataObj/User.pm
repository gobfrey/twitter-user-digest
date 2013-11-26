package TwitterSpider::DataObj::User;

use TwitterSpider::DataObj;
@ISA = ( 'TwitterSpider::DataObj' );

use strict;
use warnings;


sub harvest_option
{
	my ($self, $opt) = @_;

	my $h_c = $self->value('harvest_config_json');
	return undef unless $h_c;

	if (exists $h_c->{$opt})
	{
		return $h_c->{$opt};
	}
	return undef;
}

sub session
{
	my ($self, $spider) = @_;

	return TwitterSpider::DataObj::Session->load($spider, { id => $self->value('session_id')});
}

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

#create user records in the current session for followers
#and friends
sub create_children
{
	my ($self, $spider) = @_;
	my $session = $self->session($spider);

	foreach my $child_type (qw/ friends followers /)
	{
		my $child_ids = $self->load_extra($spider, $child_type);
		foreach my $child_id (@{$child_ids})
		{
			my $child = $session->user($spider, $child_id);
			if (!$child)
			{
				$child = $session->create_user($spider, $child_id);
			}

			#apply parameters from parent config
			my $update = 0;

			foreach my $c (qw/ friends followers tweets_from tweets_mentioning /)
			{
				if (
					$self->harvest_option($child_type . '_' . $c)
					&& $child->value($c . '_state') eq 'NO'
				)
				{
					$child->set_value($c . '_state', 'TODO');
					$update = 1;
				}

			}
			$child->commit($spider) if $update;
		}
	}
}

sub load_extra
{
	my ($self, $spider, $extra_type) = @_;

	my $db = $spider->db;
	my $user_id = $self->id;
	my $session_id = $self->value('session_id');
	my $table_name = 'user_' . $extra_type;
	my $data_col = '';

	if(
		$extra_type eq 'tweets_from'
		|| $extra_type eq 'tweets_mentioning'
	)
	{
		$data_col = 'tweets_json';
	}
	elsif
	(
		$extra_type eq 'followers'
		|| $extra_type eq 'friends'
	)
	{
		$data_col = $extra_type . '_id';
	}
	else
	{
		die "User extra $extra_type not recognised\n";
	}
	my $sql = "SELECT $data_col FROM $table_name WHERE session_id = $session_id AND user_id = $user_id";

	my $sth = $db->query($sql);
	return undef unless $sth->rows;

	#one cell of data;
	if ($data_col eq 'tweets_json')
	{
		return $db->val_from_db($data_col,$sth->fetchrow_arrayref->[0]);
	}
	#otherwise multiple cells of data
	my $data;
	while (my $row = $sth->fetchrow_arrayref)
	{
		push @{$data}, $db->val_from_db($data_col, $row->[0]);
	}
	return $data;
}

#get twitter data for this user
sub download_extra
{
	my ($self, $spider, $extra_type) = @_;
	my $twitter = $spider->twitter;
	my $screen_name = $self->value('screen_name');

	$spider->output_status("Retrieving $extra_type User Information for $screen_name...");

	my ($status, $data);

	if ($extra_type eq 'friends' || $extra_type eq 'followers')
	{
		($status, $data) = $twitter->get_friends_or_followers($screen_name, $extra_type);
	}
	elsif ($extra_type eq 'tweets_from')
	{
		($status, $data) = $twitter->tweet_search("from:$screen_name");
	}
	elsif ($extra_type eq 'tweets_mentioning')
	{
		($status, $data) = $twitter->tweet_search("\@$screen_name");
	}
	else
	{
		die "Unrecognised Data Class $extra_type\n";
	}

	return ($status, $data);
}

sub write_extra
{
	my ($self, $spider, $extra_type, $data) = @_;
	my $db = $spider->db;

	my $row = {
		'user_id' => $self->id,
		'session_id' => $self->value('session_id')
	};

	my $table_name = 'user_' . $extra_type;

	if
	(
		$extra_type eq 'tweets_from'
		|| $extra_type eq 'tweets_mentioning'
	)
	{
		$row->{tweets_json} = $data;
		$db->write($table_name, $row);
	}
	elsif
	(
		$extra_type eq 'followers'
		|| $extra_type eq 'friends'
	)
	{
		$db->write_multiple_f($extra_type, $self->value('session_id'), $self->id, $data);

#		foreach my $userid (@{$data})
#		{
#			$row->{$extra_type . '_id'} = $userid;
#			$db->write($table_name, $row, IGNORE_DUPLICATES => 1);
#		}
	}

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

			harvest_config_json MEDIUMTEXT,

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
