#!/usr/bin/perl -w
use strict;
use WWW::Salesforce::Simple;
use Data::Dumper;
use DateTime;
use SOAP::Lite +trace => 'debug';
use Date::Manip;
use Digest::MD5 qw( md5_hex );
use File::Copy;

our $force = WWW::Salesforce::Simple->new(
	username => '',
	password => '',
);


our $PERSONAL_EMAIL = 'Email';

	sub create_college {
		my %data = @_;
		$data{Type__c} = 'College/University';
		return create_school(%data);
	}

	sub create_high_school {
		my %data = @_;
		$data{Type__c} = 'High School';
		return create_school(%data);
	}

	sub create_school {
		my %data = @_;
		$data{type} = 'School__c';
		return create_record(%data);
	}

	sub create_record {
		my %data = @_;
		my $return = $force->create(%data);
		my $result = $return->envelope->{Body}->{createResponse}->{result};
		if ($result->{success} eq 'true') {
			return $return->envelope->{Body}->{createResponse}->{result}->{id};
		} else {
			warn Dumper($result->{errors});
			exit;
		}
	}

	sub create_contact {
		my %data = @_;
		$data{type} = 'Contact';
		return create_record(%data);
	}

	sub create_candidate {
		my %data = @_;
		$data{type} = 'Candidate__c';
		return create_record(%data);
	}

	sub find_candidate {
		my $email = shift;
		return get_result($force->do_query("select Id from Candidate__c where Contact__r.$PERSONAL_EMAIL = '$email'"));
	}

	sub find_school {
		my %values = fix_values(@_);
		return get_result($force->do_query("select Id from School__c where Name = '$values{Name}' and City__c = '$values{City__c}' and State__c = '$values{State__c}' and Type__c = '$values{Type__c}'"));
	}

	sub find_or_create_contact {
		if (scalar @_ % 2) {
			warn Dumper(\@_);
			exit;
		}
		my %fields = @_;
		return find_contact(%fields) || create_contact(%fields);
	}

	sub find_or_create_high_school {
		return if scalar @_ % 2;
		my %fields = @_;
		return find_high_school(%fields) || create_high_school(%fields);
	}

	sub find_or_create_college {
		return if scalar @_ % 2;
		my %fields = @_;
		return find_college(%fields) || create_college(%fields);
	}

	
	sub find_contact {
		my %values = fix_values(@_);
		return get_result($force->do_query("select id, firstname, lastname from contact where email = '$values{$PERSONAL_EMAIL}'"));
	}

sub fix_values {
	my %values = @_;
  # with no support for placeholders, we must escape apostrophes (and goodness knows what else)
	for my $key(keys(%values)) {
		$values{$key} =~ s/\'/\\\'/g;
	}
	return %values;
}

sub get_result {
	my $r = shift;
	if (my $result = shift @{$r}) {
		if (my $list = $result->{Id}) {
			if (ref($list) eq 'ARRAY') {
				return shift @{$list};
			}
		}
	} else {
		return;
	}
}

sub find_high_school {
	my %args = @_;
	$args{Type__c} = 'High School';
	return find_school(%args);
}

sub find_college {
	my %args = @_;
	$args{Type__c} = 'College/University';
	return find_school(%args);
}

sub fix_height {
	my $hash = shift;
	if ($hash->{Height} =~ /(\d+)\D* feet, (\d+)?.* inches/) {
		my $feet = $1 || '0';
		my $inches = $2 || '0';
		$hash->{Height} = ($feet * 12) + $inches;
	}
	if ( ($hash->{Height} =~ /\D/)  || ($hash->{Height} > 99) ) {
		$hash->{Height} = 70;
	}
}

my $cache = {};
our $existing_candidates = {};

#my $candidates = $force->do_query("select Contact__r.$PERSONAL_EMAIL from Candidate__c");
#for my $c (@{$candidates}) {
#	if (my $email = $c->{Contact__r}->{$PERSONAL_EMAIL}) {
#		$existing_candidates->{$email} = 1;
#	}
#}


#my @files = glob('membership_2012_mar/*.eml');
my $log_dir = '/www/cavaliers/logs/membership';
my @files = glob("$log_dir/*.log");
for my $file (@files) {
	my $hash = {};
	open(IN, $file);
	while(my $line = <IN>) {
		#chomp $line;
		$line =~ s/\x0D//g;
		$hash->{$1} = $2 if $line =~ /^\s+([\w ]+):  (\S.*)\s*$/;
		if ($line =~ /^Date: (.*)/) {
			$hash->{Questionnaire_Date} = UnixDate(ParseDate($1), "%Y-%m-%dT%H:%M:%S%z");
			print "$hash->{Questionnaire_Date}\n";
			# FIXME Date::Manip has a %N that is supposed to make an offset with a colon (-06:00), 
      # as required by the Salesforce API.  %z (used above) generates an offset without a 
 			# colon (-0600), which confuses the API (but does not raise an error).  So this line
			# adds the colon.   BS, man.
			$hash->{Questionnaire_Date} =~ s/00$/:00/; 
			print "$hash->{Questionnaire_Date}\n";
		}
	}
	close(IN);
	fix_height($hash);
	$hash->{Email} = lc($hash->{Email});
	my $checksum = md5_hex(Dumper($hash));
	if (find_candidate($hash->{Email})) {
		#warn "Skipping duplicate for $hash->{FirstName} $hash->{LastName}\n";
		move($file, "$log_dir/duplicates");
	} else {
		$cache->{$checksum} = $hash;
		if (my $candidate_id = create_all($hash)) {
			move($file, "$log_dir/archive/${candidate_id}.log");
		}
	}
}
exit;

sub create_all {
	my $hash = shift;
	if (my $candidate = $existing_candidates->{$hash->{Email}}) {
		warn "Found Candidate record for $hash->{FirstName} $hash->{LastName}\n";
		return;
	}

	#print "Creating Candidate record for $hash->{FirstName} $hash->{LastName}\n";

	my $college_id = ($hash->{College} && $hash->{CollegeState}) ? find_or_create_college( 
		Name => $hash->{College},
		City__c => $hash->{CollegeCity},
		State__c => state_name($hash->{CollegeState}),
	) : undef;

	my $high_school_id = ($hash->{HighSchool} && $hash->{HighSchoolState}) ? find_or_create_high_school( 
		Name => $hash->{HighSchool},
		City__c => $hash->{HighSchoolCity},
		State__c => state_name($hash->{HighSchoolState}),
	) : undef;

	my $phone = $hash->{Telephone};
	$phone =~ s/\D//g;
	if ($phone) {
		if ($phone =~ /^1?(\d{3})(\d{3})(\d{4})$/) {
			$phone = "($1) $2-$3";
		}
	}

	for my $i (qw(currentZIP permanentZIP)) {
		if ($hash->{$i}) {
			$hash->{$i} =~ s/[^\d\-]//g;
		}
	}

	my %contact_data = (
		$PERSONAL_EMAIL => $hash->{Email},
		#'npe01__Preferred_Email__c' => 'Personal',
		FirstName => $hash->{FirstName},
		LastName => $hash->{LastName},
		Phone => $phone,
		BirthDate => UnixDate(ParseDate($hash->{DOB} || '01/01/1970'), "%Y-%m-%d"),
		Best_Time_to_Call__c => SOAP::Data->type( string => ($hash->{'Best Time to Call'} || '') ),
		MailingStreet => $hash->{currentAddress},
		MailingCity => $hash->{currentCity},
		MailingState => $hash->{currentState},
		MailingPostalCode => $hash->{currentZIP},
	);
	if ($hash->{permanentAddress} && $hash->{permanentCity}) {
		%contact_data = (%contact_data,  
			OtherStreet => $hash->{permanentAddress},
			OtherCity => $hash->{permanentCity},
			OtherState => $hash->{permanentState},
			OtherPostalCode => $hash->{permanentZIP},
			#	OtherPhone => $hash->{permanentPhone},
		);
	}

	my $contact_id = find_or_create_contact( %contact_data );
	
	my $equip = $hash->{'Brass Instrument'} || $hash->{'Percussion Instrument'} || $hash->{'Color Guard Equipment'} || '';

	my $equip_map = {
		'Bari/Euph' => 'Baritone/Euphonium',
		Bass => 'Bass Drum',
		Mello => 'Mellophone/French Horn',
		Snare => 'Snare Drum',
		Tenor => 'Multi-tenor Drum'
	};
	if (my $mapped = $equip_map->{$equip}) {
		$equip = $mapped;
	}


	
	my @previous;
	for my $i (1..3) {
		if (my $corps = $hash->{"previous$i"}) {
			my $prev = {
				corps => $corps,
				years => $hash->{"years$i"} || ''
			};
			push(@previous, $prev);
		}
	}
	my $experience = $hash->{Experience} || '';
	if (scalar @previous) {
		$experience .= "\n\n";
		$experience .= "Previous Corps Experience:\n\n";
		for my $prev (@previous) {
			$experience .= "$prev->{corps} $prev->{years}\n";
		}
	}
	my $candidate_id = create_candidate(
		'Weight__c' => SOAP::Data->type( string => $hash->{Weight}),
		'Height__c' => SOAP::Data->type( string => $hash->{Height}),
		'Contact__c' => $contact_id,
		'High_School__c' => $high_school_id,
		'College__c' => $college_id,
		'Section__c' => $hash->{Section},
		'Instrument_Equipment__c' => $equip,
		'Experience__c' => SOAP::Data->type( string => $experience ),
		'Comments__c' => SOAP::Data->type( string => $hash->{Questions} ),
		'Questionnaire_Date__c' => SOAP::Data->type( dateTime => $hash->{'Questionnaire_Date'})
	);
	
	#print "$contact_id\n";
	#print "$candidate_id\n";
	return $candidate_id;
}
	
sub state_name {
	my $value = shift;
	if ($value =~ /^\s*\w{2}\s*$/) {
		my $states = state_hash();
		if (my $name = $states->{uc($value)}) {
			return $name;	
		}
	}
	return;
}

sub state_hash {
	return {
		AL => 'Alabama',
		AK => 'Alaska',
		AS => 'American Samoa',
		AZ => 'Arizona',
		AR => 'Arkansas',
		AA => 'Armed Forces Americas',
		AE => 'Armed Forces Europe',
		AP => 'Armed Forces Pacific',
		CA => 'California',
		CO => 'Colorado',
		CT => 'Connecticut',
		DE => 'Delaware',
		DC => 'District Of Columbia',
		FM => 'Fed. States Of Micronesia',
		FL => 'Florida',
		GA => 'Georgia',
		GU => 'Guam',
		HI => 'Hawaii',
		ID => 'Idaho',
		IL => 'Illinois',
		IN => 'Indiana',
		IA => 'Iowa',
		KS => 'Kansas',
		KY => 'Kentucky',
		LA => 'Louisiana',
		ME => 'Maine',
		MH => 'Marshall Islands',
		MD => 'Maryland',
		MA => 'Massachusetts',
		MI => 'Michigan',
		MN => 'Minnesota',
		MS => 'Mississippi',
		MO => 'Missouri',
		MT => 'Montana',
		NE => 'Nebraska',
		NV => 'Nevada',
		NH => 'New Hampshire',
		NJ => 'New Jersey',
		NM => 'New Mexico',
		NY => 'New York',
		NC => 'North Carolina',
		ND => 'North Dakota',
		MP => 'Northern Mariana Islands',
		OH => 'Ohio',
		OK => 'Oklahoma',
		OR => 'Oregon',
		PW => 'Palau',
		PA => 'Pennsylvania',
		PR => 'Puerto Rico',
		RI => 'Rhode Island',
		SC => 'South Carolina',
		SD => 'South Dakota',
		TN => 'Tennessee',
		TX => 'Texas',
		UT => 'Utah',
		VT => 'Vermont',
		VI => 'Virgin Islands',
		VA => 'Virginia',
		WK => 'Wake Island',
		WA => 'Washington',
		WV => 'West Virginia',
		WI => 'Wisconsin',
		WY => 'Wyoming',
		AB => 'Alberta',
		BC => 'British Columbia',
		MB => 'Manitoba',
		NB => 'New Brunswick',
		NF => 'Newfoundland',
		NW => 'Northwest Territories',
		NS => 'Nova Scotia',
		ON => 'Ontario',
		PE => 'Prince Edward Island',
		PQ => 'Quebec',
		SK => 'Saskatchewan',
		YT => 'Yukon Territory',
	};
}
