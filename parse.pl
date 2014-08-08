
use strict;
use warnings;

=head1 Prerequisites

Text::CSV_XS

=cut

use Text::CSV_XS;

=head1 Usage 

perl parse.pl file1 [file2 ... fileN]

=cut
my($file) = @ARGV;
my $col_ab_name_to_find = 'ab_name';
my $col_reference_id_to_find = 'reference_id';
my $dupe_abname_idx = -1;
my $dupe_refid_idx = -1;
my $dupe_line_count = 0;
my $dupe_out;
my $dupe_once;
my $dupe_many;
my $dupe_oneref;
my $dupe_abname_count = {};
my $dupe_many_abname = {};
my $csv_opts = { binary => 1, eol => $/ };
my $test = 0;

for my $file(@ARGV)
{
	&test_count_fields_per_line($file) if $test;
	#&dupe_line_per_multivalue_cell($file) unless $test;
	&dupe_multivalue($file) unless $test;
	&dupe_ab_name_once($file) unless $test;
	close $dupe_out;
	close $dupe_once;
	close $dupe_many;
	close $dupe_oneref;
	&dupe_verify_once_and_oneref_not_in_many($file) unless $test;
}
exit;

=head1 Duplicate lines with multiple values

The purpose of thie script is to transform a CSV file with the format:

	(row i) a,b,c,"1,2,3"

becomes

	(row i) a,b,c,1
	(row i+1) a,b,c,2
	(row i+2) a,b,c,3

=cut


sub dupe_multivalue
{
	my($file) = @_;
	(my $outfile = $file) =~ s/before/after/;
	open $dupe_out, ">", $outfile or die "$outfile: $!";
	open my $io, "<", $file or die "$file: $!";
	&process_file($io,\&dupe_header_cb,\&dupe_line_cb,\&dupe_field_cb);

	print "number of rows: $dupe_line_count\n";
	print "number of ab_names: " . scalar(keys %$dupe_abname_count) . "\n";
	my $only1 = 0;
	for(keys %$dupe_abname_count)
	{
		$only1 ++ if $dupe_abname_count->{$_}->{count} == 1;
	}
	print "number of ab_names seen only once: $only1\n";
	print "duplicated abnames:\n";
	for(sort keys %$dupe_abname_count)
	{
		print "$_ seen $dupe_abname_count->{$_}->{count} times\n" if $dupe_abname_count->{$_}->{count} > 1;
		#print "$_\n" if $dupe_abname_count->{$_} > 1;
	}
}

sub dupe_ab_name_once
{
	my($file) = @_;
	(my $oncefile = $file) =~ s/before/once/;
	(my $manyfile = $file) =~ s/before/many/;
	(my $onereffile = $file) =~ s/before/oneref/;
	open $dupe_once, ">", $oncefile or die "$oncefile: $!";
	open $dupe_many, ">", $manyfile or die "$manyfile: $!";
	open $dupe_oneref, ">", $onereffile or die "$onereffile: $!";
	open my $io, "<", $file or die "$file: $!";
	&process_file($io,\&dupe_header_cb,\&dupe_line_cb,\&dupe_once_field_cb);
}

sub dupe_verify_once_and_oneref_not_in_many
{
	my($file) = @_;
	(my $oncefile = $file) =~ s/before/once/;
	(my $onereffile = $file) =~ s/before/oneref/;
	(my $manyfile = $file) =~ s/before/many/;
	open $dupe_once, "<", $oncefile or die "$oncefile: $!";
	open $dupe_oneref, "<", $onereffile or die "$onereffile: $!";
	open $dupe_many, "<", $manyfile or die "$manyfile: $!";

	# undef here is for the header_cb, it's not needed because prior top-level loop dupe_* subs have parsed the header line for column indexes already
	&process_file($dupe_many,undef,\&dupe_line_cb,\&dupe_read_in_field_cb);
	print "The leftovers file has ", scalar(keys %$dupe_many_abname), " abnames\n";
	&process_file($dupe_once,undef,\&dupe_line_cb,\&dupe_verify_field_cb);
	&process_file($dupe_oneref,undef,\&dupe_line_cb,\&dupe_verify_field_cb);
}

sub dupe_header_cb
{
	my($csv,$row) = @_;
	$csv->print($dupe_out,$row);
	my $idx = -1;
	for(@$row)
	{
		$idx++;
		if(/^\s*$col_ab_name_to_find\s*$/i)
		{
			$dupe_abname_idx = $idx;
		}
		elsif(/^\s*$col_reference_id_to_find\s*$/i)
		{
			$dupe_refid_idx = $idx;
		}
		last if $dupe_abname_idx != -1 and $dupe_refid_idx != -1;
	}
	die "Didn't find $col_ab_name_to_find column\n" if $dupe_abname_idx == -1;
	die "Didn't find $col_reference_id_to_find column\n" if $dupe_refid_idx == -1;
}

sub dupe_line_cb
{
	my($csv,$row,$field_cb) = @_;
	$dupe_line_count ++ ;
	my $field = $row->[$dupe_abname_idx];
	# some fields have multiple values delimited by commas
	if(index($field,',') > -1)
	{
		for( split ',',$field )
		{
			# some fields have multiple values delimited by commas and the word 'and'
			if( /and/ )
			{
				$field_cb->($csv,$row,$_) for split 'and';
			}
			else
			{
				$field_cb->($csv,$row,$_);
			}
		}
	}
	# some fields have multiple values delimited only the word 'and'
	elsif($field =~ /\sand\s/)
	{
		$field_cb->($csv,$row,$_) for split /\sand\s/,$field;
	}
	# some fields have only one value (or at least no commas or 'and' words)
	else
	{
		if($field =~ /^\s*$/)
		{
			print "empty ab_name in row: $dupe_line_count\n";
		}
		else
		{
			$field_cb->($csv,$row,$field);
		}
	}
}

sub dupe_field_cb
{
	my($csv,$row,$field) = @_;
	$field =~ s/^\s+//;
	$field =~ s/\s+$//;
	if($field =~ /^\s*$/)
	{
		print "empty ab_name in row: $dupe_line_count\n";
	}
	$row->[$dupe_abname_idx] = $field;
	$dupe_abname_count->{$field}->{count} ++ ;
	$csv->print($dupe_out,$row);

	my $ref_field = $row->[$dupe_refid_idx];
	$ref_field =~ s/^\s+//;
	$ref_field =~ s/\s+$//;
	if(defined $ref_field and $ref_field ne '')
	{
		$dupe_abname_count->{$field}->{refs}->{$ref_field}++ ;
	}
	else
	{
		print "ref field empty in row: $dupe_line_count\n";
	}

}

sub dupe_once_field_cb
{
	my($csv,$row,$field) = @_;
	$field =~ s/^\s+//;
	$field =~ s/\s+$//;
	return if $field =~ /^\s*$/;
	if(exists $dupe_abname_count->{$field})
	{
		$row->[$dupe_abname_idx] = $field;
		my $maybe_do_many = 0;
		if( $dupe_abname_count->{$field}->{count} == 1)
		{
			$csv->print($dupe_once,$row);
			if( scalar(keys %{$dupe_abname_count->{$field}->{refs}}) != 1)
			{
				print "does this ever happen? $field\n";
			}
		}
		else
		{
			$maybe_do_many = 1;
		}
		if( scalar(keys %{$dupe_abname_count->{$field}->{refs}}) == 1)
		{
			$maybe_do_many = 0;
			$csv->print($dupe_oneref,$row);
		}
		if($maybe_do_many)
		{
			$csv->print($dupe_many,$row);
		}
	}
	else
	{
		print "Field $field, not found in dupe_abname_count!!\n";
	}
}

sub dupe_read_in_field_cb
{
	my($csv,$row,$field) = @_;
	$field =~ s/^\s+//;
	$field =~ s/\s+$//;
	return if $field =~ /^\s*$/;
	$dupe_many_abname->{$field}++;
}

sub dupe_verify_field_cb
{
	my($csv,$row,$field) = @_;
	$field =~ s/^\s+//;
	$field =~ s/\s+$//;
	return if $field =~ /^\s*$/;
	if(exists $dupe_many_abname->{$field})
	{
		print "this shouldn't happen, field $field is in many file and this file\n";
	}
}

sub process_file
{
	my($fh,$header_cb,$line_cb,$field_cb) = @_;
	my $csv = Text::CSV_XS->new($csv_opts);
	my $row = $csv->getline ($fh); ## first line is a column header names line
	$header_cb->($csv,$row) if defined $header_cb;
	
	while( $row = $csv->getline($fh) )
	{
		$line_cb->($csv,$row,$field_cb);
	}
}



=head1 Test field count per line

A test subroutine for checking that all lines, when parsed by Text::CSV_XS (with the options as set)
all have the same number of fields per line.  If some lines have a different amount of fields, then
the parsing may not be correct and you'll have to dig into changing Text::CSV_XS options, or 
dig into the file as see what's up.

Enable/disable running this test sub by setting $test to true or false at top of script.

=cut
sub test_count_fields_per_line
{
	my($file) = @_;
	my $csv = Text::CSV_XS->new($csv_opts);
	open my $io, "<", $file or die "$file: $!";
	my %n_fields;
	while (my $row = $csv->getline ($io)) 
	{
		my @fields = @$row;
		$n_fields{ scalar @fields }++;
	}
	close $io;
	print "file $file, n fields $_  found in $n_fields{$_} lines\n" for sort keys %n_fields;
	die "Error: found at least one line with a differing field count.\n" if scalar(keys %n_fields) > 1;
}
