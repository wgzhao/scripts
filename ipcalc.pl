#!/usr/bin/perl -w

# 2/2000 krischan at jodies.cx
#
# 0.14    Release
# 0.14.1  Allow netmasks given as dotted quads
# 0.15    Colorize Classbits, Mark new bits in network
# 0.16    25.9.2000 Accept <addr>/<cidr> as first argument
#         Print <pre> tag in the script
# 0.17    Bugfix
# 0.18    Replace \n with <br> in HTML to make konqueror work. Argh.
# 0.19    HTML modified again to make Internet Exploder work. Argh ** 2
#         Added -v Option
# 0.2     New Tabular Format. Idea by Kevin Ivory
# 0.21    
# 0.22    Don't show -1 if netmask is 32 (Sven Anderson)
# 0.23    Removed broken prototyping. Thanks to Scott Davis sdavis(a)austin-texas.net
# 0.31    4/1/2001 Print cisco wildcard (inverse netmask). 
#         Idea by Denis Alan Hainsworth denis(a)ans.net
# 0.32    5/21/2001 - Accepts now inverse netmask as argument (Alan's idea again)
#         Fixed missing trailing zeros in sub/supernets
#         Warns now when given netmasks are illegal
#         Added option to suppress the binary output
#         Added help text
# 0.33	  5/21/2001 Cosmetic
# 0.34    6/19/2001 Use default netmask of class when no netmask is given
# 0.35    12/2/2001 Fixed big-endian bug in subnets(). This was reported 
#         by Igor Zozulya and Steve Kent. Thank you for your help 
#         and access to your sparc machine!

use strict;

my $version = '0.35 12/2/2001';


my $private = "(Private Internet RFC 1918)";

my @privmin = qw (10.0.0.0        172.16.0.0      192.168.0.0);
my @privmax = qw (10.255.255.255  172.31.255.255  192.168.255.255);
my @class   = qw (0 8 16 24 4 5 5);

my $allhosts;
my $mark_newbits = 0;
my $print_html = 0;
my $print_bits = 1;
my $print_only_class = 0;

my $qcolor = "\033[34m"; # dotted quads, blue
my $ncolor = "\033[m";   # normal, black
my $bcolor = "\033[33m"; # binary, yellow
my $mcolor = "\033[31m"; # netmask, red
my $ccolor = "\033[35m"; # classbits, magenta
my $dcolor = "\033[32m"; # subnet bits, green
my $sfont  = "";
my $break  ="\n";

my $h;                   # Host address

foreach (@privmin) {
    $_ = &bintoint(&dqtobin("$_"));
}

foreach (@privmax) {
    $_ = &bintoint(&dqtobin("$_"));
}


if (! defined ($ARGV[0])) {
    &usage;
    exit();
}


if (defined ($ARGV[0]) && $ARGV[0] eq "-help") {
    help();
}

if (defined ($ARGV[0]) && $ARGV[0] eq "-b") {
    $ARGV[0] = "-n";
    $print_bits = 0;
}

if (defined ($ARGV[0]) && $ARGV[0] eq "-v") {
    print "$version\n";
    exit 0;
}

if (defined ($ARGV[0]) && $ARGV[0] eq "-n") {
    shift @ARGV;
    $qcolor = '';
    $ncolor = '';
    $bcolor = '';
    $mcolor = '';
    $ccolor = '';
    $dcolor = '';
    $sfont  = '';
}

if (defined ($ARGV[0]) && $ARGV[0] eq "-c") {
    shift @ARGV;
    $print_only_class = 1;
}

if (defined ($ARGV[0]) && $ARGV[0] eq "-h") {
    shift @ARGV;
    $print_html = 1;
    $qcolor = '<font color="#0000ff">' ;
    $ncolor = '<font color="#000000">';
    $bcolor = '<font color="#909090">';
    $mcolor = '<font color="#ff0000">';
    $ccolor = '<font color="#009900">';
    $dcolor = '<font color="#663366">';
    $sfont  = '</font>';
    $break  = "<br>";
    $private = "(<a href=\"http://www.ietf.org/rfc/rfc1918.txt\">Private Internet</a>)";
    print "<pre>\n";
    print "<!-- Version $version -->\n";
}


my $host  = "192.168.0.1";
my $mask  = '';
my $mask2 = '';
my @arg;


if ((defined $ARGV[0]) &&($ARGV[0] =~ /^(.+?)\/(.+)$/)) {
  $arg[0] = $1;
  $arg[1] = $2;
  if (defined($ARGV[1])) {
   $arg[2] = $ARGV[1];
  }
} else {
  @arg = @ARGV;
}

if (defined $arg[0]) {
    $host = $arg[0];
}
if (! ($host = &is_valid_dq($host)) ) {
    print "$mcolor Illegal value for ADDRESS ($arg[0])$ncolor\n";
}



if (defined $arg[1]) {
    $mask = $arg[1];
    if (! ($mask = is_valid_netmask($mask)) ) {
	print "$mcolor Illegal value for NETMASK ($arg[1])$ncolor\n";
    }
} 
else 
{
# if mask is not defined - take the default mask of the network class
   $mask = $class[getclass(dqtobin($host))];
}

if ($print_only_class) {
   print $class[getclass(dqtobin($host))];
   exit 0;
}

if (defined ($arg[2])) {
    $mask2 = $arg[2];
    if (! ($mask2 = is_valid_netmask($mask2)) ) {
	print "$mcolor Illegal value for second NETMASK ($arg[2])$ncolor\n";
    }	
} else {
    $mask2 = $mask;
} 

print "\n";

printline ("Address",   $host                      , (&dqtobin($host),$mask,$bcolor,0) );
my $m  = cidrtobin($mask);
#pack( "B*",("1" x $mask) . ("0" x (32 - $mask)) );

print_netmask($m,$mask);
print "=>\n";

$h = dqtobin($host);

my $n = $h & $m;



&printnet($n,$mask);


if ( $mask2 == $mask ) {
    &end;
}
if ($mask2 > $mask) {
    print "Subnets\n\n";
    $mark_newbits = 1;
    &subnets;
} else {
    print "Supernet\n\n";
    &supernet;
}

&end;

sub end {
 if ($print_html) {
   print "\n</pre>\n";
 }
 exit;
}

sub supernet {
    $m  = cidrtobin($mask2);
    ##pack( "B*",("1" x $mask2) . ("0" x (32 - $mask2)) );
    $n = $h & $m;
    print_netmask($m,$mask2);
    print "\n";
    printnet($n,$mask2);
}

sub subnetsREMOVED {
    my $subnets = 0;
    my @oldnet;
    my $oldnet;
    my $k;
    my @nr;
    my $nextnet;
    my $l;


    $m  = cidrtobin($mask2);
    ##pack( "B*",("1" x $mask2) . ("0" x (32 - $mask2)) );
    print_netmask($m,$mask2);
    print "\n"; #*** ??
    
    @oldnet = split //,unpack("B*",$n);
    for ($k = 0 ; $k < $mask ; $k++) {
	$oldnet .= $oldnet[$k];
    }
    for ($k = 0 ; $k < ( 2 ** ($mask2 - $mask)) ; $k++) {
	@nr = split //,unpack("b*",pack("L",$k));
	$nextnet = $oldnet;
	for ($l = 0; $l < ($mask2 - $mask) ; $l++) {
	    $nextnet .= $nr[$mask2 - $mask - $l - 1] ;
	}
	$n = pack "B32",$nextnet;
	&printnet($n,$mask2);
	++$subnets;
	if ($subnets >= 1000) {
	    print "... stopped at 1000 subnets ...$break";
	    last;
	}
    }

    if ( ($subnets < 1000) && ($mask2 > $mask) ){
	print "\nSubnets:   $qcolor$subnets $ncolor$break";
	print $ncolor . "Hosts:     $sfont$qcolor" . ($allhosts * $subnets) . "$sfont$break";
    }
}


sub subnets 
{
   my $subnet=0;
   $m  = cidrtobin($mask2);
   print_netmask($m,$mask2);
   print "\n";
  
   for ($subnet=0; $subnet < 2**($mask2 - $mask); $subnet++)
   {
     my $net = inttobin((bintoint($n) | ($subnet << (32-$mask2))));
     printnet($net,$mask2);
     if ($subnet >= 1000) {
        print "... stopped at 1000 subnets ...$break";
	return;
     }
   }
   if ($mask2 > $mask) {
      print "\nSubnets:   $qcolor$subnet $ncolor$break";
      print "Hosts:     $qcolor" . ($allhosts * $subnet) . "$ncolor$break";
   }
}

sub print_netmask {
   my ($m,$mask2) = @_;
   printline ("Netmask",        &bintodq($m) . " = $mask2", ($m,$mask2,$mcolor,0) );
   printline ("Wildcard",       &bintodq(~$m)              , (~$m,$mask2,$bcolor,0) );
}

sub getclass {
   my $n = $_[0];
   my $class = 1;
   while (unpack("B$class",$n) !~ /0/) {
      $class++;
      if ($class > 5) {
	 last;
      }
   } 
   return $class;
}

sub printnet {
    my ($n,$mask) = @_;
    my $nm;
    my $type;
    my $hmin;
    my $hmax; 
    my $hostn;
    my $p;
    my $i;

    
    ## $m  = pack( "B*",("1" x $mask) . ("0" x (32 - $mask)) );
    $nm = ~cidrtobin($mask);
    ##pack( "B*",("0" x $mask) . ("1" x (32 - $mask)) );

    $b = $n | $nm;
    
    #$type = 1;
    #while (unpack("B$type",$n) !~ /0/) {
    #	$type++;
    #}
    #if ($type > 5) {
    #	$type = '';
    #} else {
    #	$type = "Class " . chr($type+64);
    #}
    
    $type = getclass($n);
    if ($type > 5 ) {
       $type = "Undefined Class";
    } else {
       $type = "Class " . chr($type+64);
    }
    
    $hmin  = pack("B*",("0"x31) . "1") | $n;
    $hmax  = pack("B*",("0"x $mask) . ("1" x (31 - $mask)) . "0" ) | $n;
    $hostn = (2 ** (32 - $mask)) -2  ;
    
    $hostn = 1 if $hostn == -1;
    

    $p = 0;
    for ($i=0; $i<3; $i++) {
	if ( (&bintoint($hmax) <= $privmax[$i])  && 
             (&bintoint($hmin) >= $privmin[$i]) ) {
	    $p = $i +1;
	    last;
	}
    }
    
    if ($p) {
	$p = $private;
    } else {
	$p = '';
    }


    printline ("Network",   &bintodq($n) . "/$mask", ($n,$mask,$bcolor,1),  " ($ccolor" . $type. "$ncolor)" );
    printline ("Broadcast", &bintodq($b)           , ($b,$mask,$bcolor,0) );
    printline ("HostMin",   &bintodq($hmin)        , ($hmin,$mask,$bcolor,0) );
    printline ("HostMax",   &bintodq($hmax)        , ($hmax,$mask,$bcolor,0) );
    printf $ncolor . "Hosts/Net: $sfont$qcolor%-22s",$hostn;
    print $sfont . $ncolor;
    
    if ($p) {
       print "$p";
    }
   
    print "$break$break\n";
   
    $allhosts = $hostn;
}

sub printline {
   my ($label,$dq,$mask,$mask2,$color,$mark_classbit,$class) = @_;
   $class = "" unless $class;
   printf "$ncolor%-11s$sfont$qcolor","$label:";
   printf "%-22s$sfont", "$dq";
   if ($print_bits) 
   {
      print  formatbin($mask,$mask2,$color,$mark_classbit);
      if ($class) {
         print $class;
      }
   }
   print $sfont . $break;
}

sub formatbin {
    my ($bin,$actual_mask,$color,$mark_classbits) = @_;
    my @dq;
    my $dq;
    my @dq2;
    my $is_classbit = 1;
    my $bit;
    my $i;
    my $j;
    my $oldmask;
    my $newmask;

    if ($mask2 > $mask) {
	$oldmask = $mask;
	$newmask = $mask2;
	
    } else {
	$oldmask = $mask2;
	$newmask = $mask;
    }



    @dq = split //,unpack("B*",$bin);
    if ($mark_classbits) {
	$dq = $ccolor;
    }	else {
	$dq = $color;
    }
    for ($j = 0; $j < 4 ; $j++) {
	for ($i = 0; $i < 8; $i++) {
	    if (! defined ($bit = $dq[$i+($j*8)]) ) {
		$bit = '0';
	    }

	    if ( $mark_newbits &&((($j*8) + $i + 1) == ($oldmask + 1)) ) {
		$dq .= "$dcolor";
	    }


	    $dq .= $bit;
	    if ( ($mark_classbits && 
		  $is_classbit && $bit == 0)) {
		$dq .= $color;
		$is_classbit = 0;
	    }
	    
	    if ( (($j*8) + $i + 1) == $actual_mask ) {
		$dq .= " ";
	    }

	    if ( $mark_newbits &&((($j*8) + $i + 1) == $newmask) ) {
		$dq .= "$color";
	    }

	}
	push @dq2, $dq;
	$dq = '';
    }
    return (join ".",@dq2);
}

sub dqtobin {
        my @dq;
	my $q;
	my $i;
	my $bin;

	foreach $q (split /\./,$_[0]) {
		push @dq,$q;
	}
	for ($i = 0; $i < 4 ; $i++) {
		if (! defined $dq[$i]) {
			push @dq,0;
		}
	}
	$bin    = pack("CCCC",@dq);      # 4 unsigned chars
	return $bin;
}

sub bintodq {
	my $dq = join ".",unpack("CCCC",$_[0]);
print 
	return $dq;
}

sub inttobin {
        return pack("N",$_[0]);
}

sub bintoint {
	return unpack("N",$_[0]);
}


sub is_valid_dq {
	my $value = $_[0];
	my $test = $value;
 	my $i;
	my $corrected;
	$test =~ s/\.//g;
	if ($test !~ /^\d+$/) {
		return 0;
	}
	my @value = split /\./, $value, 4;
	for ($i = 0; $i<4; $i++) {
		if (! defined ($value[$i]) ) {
			$value[$i] = 0;
		}
		if ( ($value[$i] !~ /^\d+$/) ||
		     ($value[$i] < 0) || 
                     ($value[$i] > 255) ) 
                {
			return 0;
		}
	}
	$corrected = join ".", @value;
	return $corrected;
}

sub is_valid_netmask {
	my $mask = $_[0];
	if ($mask =~ /^\d+$/) {
		if ( ($mask > 32) || ($mask < 1) ) {
			return 0;
		}
	} else {
		if (! ($mask = &is_valid_dq($mask)) ) {
			return 0;
		}
		$mask = dqtocidr($mask);
	}
	return $mask;

}


sub cidrtobin {
   my $cidr = $_[0];
   pack( "B*",(1 x $cidr) . (0 x (32 - $cidr)) );
}

sub dqtocidr {
	my $dq = $_[0];
	$b = &dqtobin($dq);
	my $cidr = 1;
	my $firstbit = unpack("B1",$b) ^ 1;
	while (unpack("B$cidr",$b) !~ /$firstbit/) {
		$cidr++;
		last if ($cidr == 33);
	}
	$cidr--;
	#print "CIDR: $cidr\n";
	#print "DQ:  $dq\n";
	my $m = cidrtobin($cidr);
	#print "NM:  " . bintodq($m) . "\n";
	#print "NM2: " . bintodq(~$m) . "\n";
	if (bintodq($m) ne $dq && bintodq(~$m) ne $dq) {
 	   print "$mcolor Corrected illegal netmask: $dq" . "$ncolor\n";
	}
	return $cidr;
	
}

sub usage {
    print << "EOF";
Usage: ipcalc [-n|-h|-v|-help] <ADDRESS>[[/]<NETMASK>] [NETMASK]

ipcalc takes an IP address and netmask and calculates the resulting broadcast, 
network, Cisco wildcard mask, and host range. By giving a second netmask, you 
can design sub- and supernetworks. It is also intended to be a teaching tool 
and presents the results as easy-to-understand binary values. 

 
 -n    Don't display ANSI color codes
 -b    Suppress the bitwise output
 -c    Just print bit-count-mask of given address
 -h    Display results as HTML
 -help Longer help text
 -v    Print Version
 
Examples:

ipcalc 192.168.0.1/24
ipcalc 192.168.0.1/255.255.128.0
ipcalc 192.168.0.1 255.255.128.0 255.255.192.0
ipcalc 192.168.0.1 0.0.63.255

EOF
}

sub help {
    print << "EOF";
    
IP Calculator $version

Enter your netmask(s) in CIDR notation (/25) or dotted decimals (255.255.255.0).
Inverse netmask are recognized. If you mmit the netmask, ipcalc uses the default
netmask for the class of your network.

Look at the space between the bits of the addresses: The bits before it are 
the network part of the address, the bits after it are the host part. You can
see two simple facts: In a network address all host bits are zero, in a 
broadcast address they are all set. 

The class of your network is determined by its first bits. 

If your network is a private internet according to RFC 1918 this is remarked. 
When displaying subnets the new bits in the network part of the netmask are 
marked in a different color. 

The wildcard is the inverse netmask as used for access control lists in Cisco 
routers. You can also enter netmasks in wildcard notation. 

Do you want to split your network into subnets? Enter the address and netmask 
of your original network and play with the second netmask until the result 
matches your needs. 


Questions? Comments? Drop me a mail... 
krischan at jodies.de
http://jodies.de/ipcalc

Thanks for your nice ideas and help to make this tool more useful: 

Hermann J. Beckers   hj.beckers(a)kreis-steinfurt.de
Kevin Ivory          ki(a)sernet.de
Frank Quotschalla    gutschy(a)netzwerkinfo.de
Sven Anderson        sven(a)anderson.de
Scott Davis          sdavis(a)austin-texas.net
Denis A. Hainsworth  denis(a)ans.net
Steve Kent           stevek(a)onshore.com
Igor Zozulya         izozulya(a)yahoo.com

    
EOF
usage();
exit;
}
