#!/usr/bin/perl

use strict;
use warnings;

use lib "/home/af05v/twitter-user-digest/perl_lib";
use CGI;
use Template;

use TwitterSpider;

my $cfg_file = '/home/af05v/twitter-user-digest/twitter_user_digest_conf.ini'; 

#move to config_file
my $script_url = 'http://slapstick.ecs.soton.ac.uk/snapshot/snapshot.cgi';
my $template_path = '/home/af05v/twitter-user-digest/templates'; 

my $ts = TwitterSpider->new($cfg_file);
my $cgi = CGI->new;
my $template = Template->new({
	INCLUDE_PATH => $template_path,  # or list ref
	PRE_PROCESS  => 'config',        # prefix each template
});
my $data = {}; #data that will be passed into the template
my $template_file = '';

my $session_id = $cgi->param('session');
my $user_id = $cgi->param('user');


if ($session_id && $user_id)
{
	#render user
	my $session = TwitterSpider::DataObj::Session->load($ts, {id => $session_id});
	$data->{session} = $session;
	$data->{user} = $session->user($ts, $user_id);
	$template_file = 'user_in_session.html';
}
elsif ($session_id)
{
	#display list of clickable root users
	my $session = TwitterSpider::DataObj::Session->load($ts, {id => $session_id});
	$data->{session} = $session;
	$data->{users} = TwitterSpider::DataObj::User::load_all_root_users($ts,$session);
	$template_file = 'users_in_session.html';

}
elsif ($user_id)
{
	#display list of sessions this user appears in
	$data->{sessions} = TwitterSpider::DataObj::Session::load_all_sessions($ts, $user_id);
}
else
{
	$data->{users} = TwitterSpider::DataObj::User::load_all_root_users($ts);
	$template_file = 'page_root.html'; 
}

print $cgi->header; 
$template->process($template_file,$data)
	 || die $template->error();

