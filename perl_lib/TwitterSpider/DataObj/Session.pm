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

#gets JSON data for all users in the current session that don't have it yet
sub download_user_data
{
	my ($self, $spider) = @_;
	my $db = $spider->db;
	my $twitter = $spider->twitter;

	while (1) #exit points on returns below
	{
		my $sql = 'SELECT id FROM user WHERE session_id=' . $self->id;
		$sql .= ' AND user_data_state =\'TODO\' LIMIT 100'; 

		my $sth = $db->query($sql);
		my @ids;
		while (my $row = $sth->fetchrow_arrayref)
		{
			push @ids, $row->[0];
		}

		#if we have no IDS, then all in the database are good
		if (!scalar @ids)
		{
			#####EXIT POINT
			return 'complete';
		}

		my ($status, $users) = $twitter->query('lookup_users', {'user_id' => \@ids, include_entities => 1});

		#####EXIT POINT
		return 'incomplete' unless $status == 200; #probably out of API

		foreach my $user_data (@{$users})
		{
			my $user_obj = $self->user($spider, $user_data->{'id'});
			die "Unable to load user " . $user_data->{'user_id'} . "\n" unless $user_obj;

			$user_obj->set_value('screen_name', $user_data->{'screen_name'});
			$user_obj->set_value('user_data_state',  'OK');
			$user_obj->set_value('user_data_json', $user_data);

			$user_obj->commit($spider);
		}

		#####EXIT POINT
		return 'incomplete' if $status != 200; #problem with the API, exit here
	}
}

#for every user in the current session, download the bits that the user needs
sub download_user_extras
{
	my ($self, $spider) = @_;
	my $db = $spider->db;
	my $twitter = $spider->twitter;

	my $complete = 1;
	EXTRA_TYPE: foreach my $extra_type (qw/ friends followers tweets_from tweets_mentioning /)
	{
		#this leads to more SQL, but it makes sense to do it this way as we want to do it by
		#twitter API call
		my $sql = 'SELECT id FROM user WHERE user_data_state = \'OK\' AND session_id = ' . $self->id;
		my $sth = $db->query($sql);
		USER: while (my $row = $sth->fetchrow_arrayref)
		{
			my $user = $self->user($spider,$row->[0]);
			next if $user->value($extra_type . '_state') ne 'TODO';

			#if the user is private, we'll get no data
			my $user_data = $user->value('user_data_json');
			if (
				exists $user_data->{protected}
				&& $user_data->{protected}
			)
			{
				$user->set_value($extra_type . '_state', 'PRIVATE');
				$user->commit($spider);
				next USER;
			}

			my $screen_name = $user->value('screen_name');

			$spider->output_status("Enriching $extra_type for $screen_name");

			my ($status, $data);
			if (
				($extra_type eq 'frields' || $extra_type eq 'followers')
				&& $user_data->{$extra_type . '_count'} > (15 * 5000) #the max accessible in a single window
			)
			{
				$user->set_value($extra_type . '_state', 'TOOMANY');
				$user->commit($spider);
				next USER;
			}

			($status, $data) = $user->download_extra($spider, $extra_type);

			if ($status == 429) # Out of API
			{
				$spider->output_status("Out of API for $extra_type");
				$complete = 0;
				next EXTRA_TYPE; #we're probably out of API for this type
			}
			elsif ($status >= 500 && $status < 600)
			{
				print STDERR "$status: terminating";
				last EXTRA_TYPE; #exit just to be safe
			}
			elsif ($status != 200) #It's probably a permissions error
			{
				$spider->output_status("HTTP STATUS $status for $extra_type for $screen_name.");
				$user->set_value($extra_type . '_state', "ERR$status");
				next USER;
			}	

			#status must be 200.  All OK
			$user->write_extra($spider, $extra_type, $data);
			$user->set_value($extra_type . '_state', 'OK');
			$user->commit($spider);
		}
	}

	if ($complete)
	{
		return 'complete';
	}
	return 'incomplete';
}

#parents are unimportant here because we will never download
#extra data into these.
sub create_spider_children
{
	my ($self, $spider) = @_;

	foreach my $f (qw/ friends followers /)
	{
		my $col = $f . '_id';
		my $table = "user_$f";
		my $sql = "SELECT $col FROM $table session_id = " . $self->id;
		my $sth = db_query($sql);
		while (my $row = $sth->fetchrow_arrayref)
		{
			my $child_user = $self->user($spider,$row->[0]);
			if (!$child_user)
			{
				$child_user = $self->create_user($spider, $row->[0]);
			}
		}
	}



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
