#!/usr/bin/perl

use strict;
use warnings;

use lib "/home/af05v/twitter-user-digest/perl_lib";
use CGI;
use Template;
use JSON;
use TwitterSpider;

my $cfg_file = '/home/af05v/twitter-user-digest/twitter_user_digest_conf.ini'; 

my $cgi = CGI->new;
my $session_id = $cgi->param('session');
my $user_id = $cgi->param('user');

my $ts = TwitterSpider->new($cfg_file, $session_id, $user_id);

print STDERR $ts->config->value('system','template_path');

my $template = Template->new({
	INCLUDE_PATH => $ts->config->value('system','template_path'),  # or list ref
	PRE_PROCESS  => 'config',        # prefix each template
});
my $template_file = '';

my $data = {}; #data that will be passed into the template
        
if ($session_id && $user_id)
{
	$data = TwitterSpider::PageContent::prepare_user_in_session($ts, $session_id, $user_id);
	$data->{timeline_height} = '150px';
	$template_file = 'user_in_session.html';
}
#elsif ($session_id)
#{
#	#display list of clickable root users
#	my $session = TwitterSpider::DataObj::Session->load($ts, {id => $session_id});
#	$data->{session} = $session;
#	$data->{users} = TwitterSpider::DataObj::User::load_all_root_users($ts,$session);
#	$template_file = 'users_in_session.html';
#
#}
#elsif ($user_id)
#{
#	#display list of sessions this user appears in
#	$data->{sessions} = TwitterSpider::DataObj::User::load_sessions_data($ts, $user_id);
#	$template_file = 'users_sessions.html';
#}
else
{
	$data = TwitterSpider::PageContent::prepare_home($ts);
	$data->{timeline_height} = '350px';
	$template_file = 'page_root.html'; 
}

$data->{base_url} = $ts->config->value('system','base_url');
$data->{script_url} = $ts->config->value('system', 'script_url');
$data->{session_id} = $session_id;
$data->{user_id} = $user_id;

print $cgi->header(-charset => 'UTF-8'); 
$template->process($template_file,$data)
	 || die $template->error();

