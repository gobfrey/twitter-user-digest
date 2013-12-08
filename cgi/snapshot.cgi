#!/usr/bin/perl

use strict;
use warnings;

use lib "/home/af05v/twitter-user-digest/perl_lib";
use CGI;

use TwitterSpider;

my $cfg_file = '/home/af05v/twitter-user-digest/twitter_user_digest_conf.ini'; 

my $ts = TwitterSpider->new($cfg_file);
my $cgi = CGI->new;

my $session_id = $cgi->param('session');
my $user_id = $cgi->param('user');

print $cgi->header;
print $cgi->start_html('Twitter Snapshoterer');
print $cgi->h1('TEST');

if ($session_id && $user_id)
{
	#render user
	my $session = TwitterSpider::DataObj::Session->new($ts, {id => $session_id});
	exit unless $session;
	my $user = $session->user($ts, $user_id);

	print $user->render_info($ts);

}
elsif ($session_id)
{
	#display list of clickable root users

}
elsif ($user_id)
{
	#display list of sessions this user appears in

}
else
{
	print $cgi->p('Select a user to browse');
	#display list of sessions AND list of all root users
	print TwitterSpider::DataObj::User::render_all_root_users_list($ts);



}


print $cgi->end_html();


