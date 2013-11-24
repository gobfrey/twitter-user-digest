package TwitterSpider::DB;

use strict;
use warnings;

use JSON;
use DBI;

sub new
{
	my ($class, $spider) = @_;

	my $data = {}; #will become the object

	$data->{spider} = $spider;

	return bless $data, $class;
}


sub connect
{
	my ($self) = @_;
	my $spider = $self->{spider};
	my $cfg = $spider->config;

	$spider->output_status('connecting to database');

	my $database = $cfg->cfg('database','db_name');
	my $hostname = $cfg->cfg('database','db_host');
	my $port = $cfg->cfg('database','db_port');
	my $password = $cfg->secret('database','db_password');
	my $user = $cfg->secret('database','db_username');

	my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";

	$self->{dbi} = DBI->connect($dsn, $user, $password)
                or die "Couldn't connect to database: " . DBI->errstr;

	$self->initialise_db;
}

sub initialise_db
{
	my ($self) = @_;

	my $queries = [];
	foreach my $class (qw/ Session User /)
	{
		my $class_obj = bless {}, 'TwitterSpider::DataObj::' . $class;
		my $class_queries = $class_obj->mysql_tabledef;
		@{$queries} = (@{$queries}, @{$class_queries});
	}

	$self->query($_) foreach @{$queries};
}

sub query
{
	my ($self, $sql, @args) = @_;
	my $spider = $self->{spider};

	$self->connect unless $self->{dbi}; 
	my $dbi = $self->{dbi};

	$spider->output_status("Running $sql");

	my $sth = $dbi->prepare($sql)
		or die "Couldn't prepare statement: " . $dbi->errstr;

	$sth->execute(@args)
		or die "Couldn't execute statement: " . $sth->errstr;

	return $sth;
}

#adds a new row to the database
sub write
{
	my ($self, $table_name, $hashref, %opts) = @_;

	my @colnames;
	my @values;
	my @questionmarks;

	foreach my $k (keys %{$hashref})
	{
		push @colnames, "`$k`";
		push @values, $self->val_for_db($k,$hashref->{$k});
		push @questionmarks, '?';
	}

	my $sql = "INSERT INTO $table_name (" . join(', ',@colnames) . ') VALUES (' . join(', ',@questionmarks) . ')';
	if ($opts{IGNORE_DUPLICATES})
	{
		my $c = $colnames[0];
		$sql .= " ON DUPLICATE KEY UPDATE $c=$c";
	}

	$self->query($sql, @values);
}

sub val_for_db
{
	my ($self, $fieldname, $value) = @_;

	if ($fieldname =~ m/_json$/)
	{
		return '' unless ref $value; #should be a hashref or an arrayref
		return encode_json $value;
	}
	return $value
}

#if it's a JSON field, decode it
sub val_from_db
{
	my ($self, $fieldname, $value) = @_;

	if ($fieldname =~ m/_json$/)
	{
		return undef unless $value; #should do proper error handling
		return decode_json $value;
	}
	return $value
}

#process values from database
sub obj_from_db
{
	my ($self, $hashref) = @_;

	return undef unless $hashref;

	foreach my $k (keys %{$hashref})
	{
		$hashref->{$k} = $self->val_from_db($k, $hashref->{$k});
	}
	return $hashref;
}

sub update
{
	my ($self, $table_name, $hashref) = @_;

	die ("Cannot update with an id set") unless $hashref->{id};
	if ($table_name eq 'user')
	{
		die "Cannot update a user without a session_id" unless $hashref->{session_id};
	}

	my @bits;
	my @values;
	foreach my $k (keys %{$hashref})
	{
		next if $k eq 'id';
		next if $k eq 'session_id';
		push @bits, "`$k`=?";
		push @values, $self->val_for_db($k,$hashref->{$k});
	}

	my $sql = "UPDATE $table_name SET " . join(', ', @bits) . " WHERE `id`=?";
	push @values, $hashref->{id};
	if ($hashref->{session_id})
	{
		$sql .= ' AND `session_id`=?';
		push @values, $hashref->{session_id};
	}

	$self->query($sql, @values);
}



1;
