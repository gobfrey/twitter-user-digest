package TwitterSpider::DataObj;

use strict;
use warnings;



sub create
{
	die "DataObj::create needs to be subclassed\n";
}

sub load
{
	my ($class, $spider, $id_bits) = @_;

	my $db = $spider->db;

	my $class_obj = bless {}, $class; #a fake object to make calling class functions easier
	my $sql = $class_obj->_load_sql($id_bits);
	$class_obj = undef;

	#collect values for parameterised query
	my @id_vals;
	my $id_fields = $class_obj->id_fields;
	foreach my $f (@{$id_fields})
	{
		push @id_vals, $id_bits->{$f};
	} 

	my $sth = $db->query($sql, @id_vals); 
	if (!$sth->rows)
	{
		return undef;
	}

	my $user = $sth->fetchrow_hashref;

	return bless $db->obj_from_db($user), $class;
}

sub _load_sql
{
	my ($self, $id_bits) = @_;	

	my $table = $self->class_id;
	my $id_fields = $self->id_fields;

	my @sql_where_bits;

	foreach my $f (@{$id_fields})
	{
		die "missing ID field data $f when creating a $table\n" unless exists $id_bits->{$f};
		push (@sql_where_bits, $f . " = ? ");
	}

	my $sql = "SELECT * FROM " . $table ." WHERE " . join(' AND ', @sql_where_bits);

	return $sql;
}

sub has_field
{
	my ($self, $k) = @_;

	return 1 if exists $self->{$k};
	return 0;
}

sub id
{
	my ($self) = @_;
	return $self->value('id');
}

sub value
{
	my ($self, $k) = @_;

	return undef unless $self->has_field($k);

	return $self->{$k};
}

sub set_value
{
	my ($self, $k, $v) = @_;

	die "Attemt to set non-existant value $k on " . $self->class_id . "\n" unless $self->has_field($k);

	return $self->{$k} = $v;
}

sub commit
{
	my ($self, $spider) = @_;
	my $db = $spider->db;

	if ($self->exists_in_db($spider))
	{
		$db->update($self->class_id, $self);
	}
	else
	{
		$db->write($self->class_id, $self);
	}
}

sub exists_in_db
{
	my ($self, $spider) = @_;
	my $db = $spider->db;

	#test if it exists in the database
	my $id_fields = $self->id_fields;
	my $id_bits = {};
	my @id_vals;
	foreach my $f (@{$id_fields})
	{
		return 0 if !$self->value($f);
		$id_bits->{$f} = $self->value($f);	
		push @id_vals, $self->value($f);
	}

	my $sql = $self->_load_sql($id_bits);

	my $sth = $db->query($sql, @id_vals); 
	if ($sth->rows)
	{
		return 1;
	}
	return 0;
}


1;

