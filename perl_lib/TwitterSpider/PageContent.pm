package TwitterSpider::PageContent;
use strict;
use warnings;

use JSON;

#functions to prepare data for inclusion on pages at the web front-end

sub prepare_home
{
	my ($spider) = @_;

	my $cfg = $spider->config;
	my $data = {};

	#get session ids and root users
	my $sql = 'SELECT session.id, session.start_time, session.end_time, user.id, user.screen_name ';
	$sql .= 'FROM user JOIN session ON session.id = user.session_id ';
	$sql .= 'WHERE session.status = "complete" AND user.harvest_root = 1 ';
	$sql .= 'ORDER BY session.id, user.screen_name';

	my $sth = $spider->db->query($sql);

	my $sessions = {};

	#construct timeline event boxes
	while (my @row = $sth->fetchrow_array)
	{
		$sessions->{$row[0]}->{type} = 'box';
		$sessions->{$row[0]}->{id} = $row[0];
		$sessions->{$row[0]}->{start} = $row[1];
		$sessions->{$row[0]}->{end} = $row[2];
		#we'll need to convert this array into an html string later
		push @{$sessions->{$row[0]}->{_users}},
		{
			id => $row[3],
			screen_name => $row[4]
		};
	}

	my @timeline;
	foreach my $id (sort keys %{$sessions})
	{
		my $session = $sessions->{$id};

		#create html content
		my $html = ''; #"<strong>Session $id</strong>";
		$html .= "<ul class='unstyled'>";
		foreach my $user (@{$session->{_users}})
		{
			$html .= '<li>';
			$html .= _user_in_session_link($spider, $id, $user->{id}, $user->{screen_name});
			$html .= '</li>';
		}
		$html .= "</ul>";
		$session->{content} = $html;
		push @timeline, $session;
	}
	$data->{timeline_nodes} = encode_json(\@timeline);

	return $data;
}

sub prepare_user_in_session
{
	my ($spider, $session_id, $user_id) = @_;

	my $data = {};
	my $session = TwitterSpider::DataObj::Session->load($spider, {id => $session_id});
	my $user = $session->user($spider, $user_id);

	$data->{session} = $session;
	$data->{user} = $user;
	$data->{user_followers} = TwitterSpider::PageContent::user_list($spider, $user, 'followers');
	$data->{user_friends} = TwitterSpider::PageContent::user_list($spider, $user, 'friends');

	$data->{user_tweets_from} = $user->load_extra($spider,'tweets_from');
	$data->{user_tweets_mentioning} = $user->load_extra($spider,'tweets_mentioning');

	#count how many instances of each user are in the friends and followers lists
	foreach my $u (@{$data->{user_followers}}, @{$data->{user_friends}})
	{
		$data->{ff_counts}->{$u->{id}}++;
	}

	#get user's state from previous session
	$data->{prev_user} = $user->load_previous($spider);
	$data->{previous_session_id} = $user->previous_session_id($spider);
	$data->{next_session_id} = $user->next_session_id($spider);

	$data->{flags}->{prev_user} = 0;
	if ($data->{prev_user})
	{
		$data->{flags}->{prev_user} = 1;

		foreach my $extra (qw/ friends followers /)
		{
			my $old_users_list = TwitterSpider::PageContent::user_list($spider, $data->{prev_user}, $extra);			
			#create hash keyed on user IDs
			my %old = map { $_->{id} => 1 } @{$old_users_list};		
			my %current = map { $_->{id} => 1 } @{$data->{'user_' . $extra}};		

			$data->{'user_' . $extra . '_new'} = [];
			foreach my $user (@{$data->{'user_'.$extra}})
			{
				if (!$old{$user->{id}})
				{
					push @{$data->{'user_' . $extra . '_new'}}, $user;
				}	
			}

			$data->{'user_' . $extra . '_gone'} = [];
			foreach my $user (@{$old_users_list})
			{
				if (!$current{$user->{id}})
				{
					push @{$data->{'user_' . $extra . '_gone'}}, $user;
				}	
			}
		}

	}

	#populate timeline of sessions this user appears in
	my $sql = 'SELECT session.id, session.start_time, session.end_time ';
	$sql .= 'FROM session JOIN user ON session.id = user.session_id ';
	$sql .= 'WHERE user.id = ' . $user->id . ' ';
	$sql .= 'ORDER BY session.id';

	my $sth = $spider->db->query($sql);

	my $timeline = [];

	while (my $row = $sth->fetchrow_arrayref)
	{
		my $content = $row->[0];
		my $class_name = 'selected';
		#the general case -- this one isn't selected
		if ($spider->session_id != $row->[0])
		{
			$class_name = 'unselected';
			$content = _user_in_session_link($spider, $row->[0], $user->id, $row->[0]);
		}
		push @{$timeline}, {
			type => 'box',
			id => $row->[0],
			start => $row->[1],
			end => $row->[2],
			className => $class_name,
			content => $content
		};
	}

	$data->{timeline_nodes} = encode_json($timeline);

	return $data;
}

sub _user_in_session_link
{
	my ($spider, $session_id, $user_id, $text) = @_;

	my $url = $spider->config->value('system','script_url');
	$url .= '?user=' . $user_id;
	$url .= '&session=' . $session_id;
	return "<a href='$url'>$text</a>";
}

#loads a friends or followers list with id and screen_name
#$extra is 'friends' or 'followers'
sub user_list
{
	my ($spider, $user, $extra) = @_;

	my $table_name = "user_$extra";
	my $userid_column = $extra . '_id';
	my $session_id = $user->value('session_id');
	my $user_id = $user->id;

	my $sql = "SELECT $table_name.$userid_column AS id, user.screen_name";
	$sql .= " FROM $table_name JOIN user";
	$sql .= " ON user.id = $table_name.$userid_column";
	$sql .= " AND $table_name.session_id = user.session_id";
	$sql .= " WHERE $table_name.session_id = $session_id";
	$sql .= " AND $table_name.user_id = $user_id";
	$sql .= " ORDER BY user.screen_name";

	return $spider->db->selectall_arrayref($sql);
}

1;
