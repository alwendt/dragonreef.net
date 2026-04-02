package FileParser;
#┌────────────────────────────────► INFO ◄─────────────────────────────────┐
#│THE COMMENTS ON THIS PAGE ARE UTF-8 ENCODED.                             │
#│You may wish to change your terminal client settings or your IDE         │
#│settings to prevent jibberish from apearing on your screen.              │
#└─────────────────────────────────────────────────────────────────────────┘
use strict;
use base 'Exporter';
our @EXPORT_OK = qw(new
                    setHTML
                    getHTML
                    parseHTML
                    printHTML);

#                         ╔══════════════════════════╗
#╔════════════════════════╣ File Manager HTML PARSER ╠═══════════════════════╗
#║                        ╚══════════════════════════╝                       ║
#║Version 1.1, August 25, 2003                                               ║
#║Created by Phill Babbitt (pbabbitt@gmx.net)                                ║
#║Copyright ©2003 Phill Babbitt                                              ║
#║──────                                                                     ║
#║An HTML parser that mimics good MVC jsp (to make me feel at home working   ║
#║in Perl!)                                                                  ║
#║                                                                           ║
#║Current limitations:                                                       ║
#║1. You cannot have multiple iterators on the same line                     ║
#║2. You cannot have an iterator and a conditional on the same line          ║
#║3. You cannot have an iterator within a conditional                        ║
#║4. You cannot have a conditional within an iterator                        ║
#║5. The prefix needs to be on it's own line                                 ║
#║                                                                           ║
#║These limitations (except for 5... that one seems like it should stay)     ║
#║will be solved as I have more time to work on the project.  Until then,    ║
#║allow this module to help you achieve MVC-based developement.              ║
#║                                                                           ║
#║For more information on MVC architecture, check out the following:         ║
#║  http://www.object-arts.com/EducationCentre/Overviews/MVC.htm             ║
#║  http://www.comptechdoc.org/independent/mvc/weblinks/index.html           ║
#║  http://st-www.cs.uiuc.edu/users/smarch/st-docs/mvc.html                  ║
#║                                                                           ║
#║MVC was originally conceived for Smalltalk-80, but is a perfect fit for    ║
#║achitecting web applications!  Let the engineers code and the GUI          ║
#║developers design!  Keep out of eachother's domain and we'll all be        ║
#║happier!                                                                   ║
#║                                                                           ║
#║─HOW IT WORKS────────────                                                  ║
#║Stuff                                                                      ║
#╚═══════════════════════════════════════════════════════════════════════════╝

#┌ new ────────────────────────────────────────────────────────────────────────┐
#│A new instance of the File Manager HTML Parser can be created in one of two  │
#│ways:                                                                        │ 
#│ •new()                                                                      │
#│  This creates the instance with default parameters.  A call to              │
#│  setHTML(@HTML) will be required afterward however.                         │
#│ •new(@HTML)                                                                 │
#│  If an array containing the HTML is provided upon instantiation, a later    │
#│  call to setHTML(@HTML) will not be required as the File Manager HTML       │
#│  parser object will already have the HTML in place.                         │
#└─────────────────────────────────────────────────────────────────────────────┘
sub new {
  my $class = shift;
  my (@HTML, %available_refs);
  
  if (@_ > 1) {
    @HTML = @_;
  } elsif (@_ > 0) {
    my $filename = shift;
    
    open(HTML, $filename) or die "Can't open $filename: $!";
    @HTML = <HTML>;
    close(HTML);
  }
  
  my $self = bless {
             ref_array       => 'ARRAY',
             ref_hash        => 'HASH',
             ref_scalar      => 'SCALAR',
             setup_tag_open  => '<%@ ts',
             setup_tag_close => '%>',
             prefix_atrib    => 'prefix',
             iterator_tag    => 'iterator',
             true            => 1,
             false           => 0,
             prefix          => undef,
             iterator_start  => undef,
             iterator_end    => undef,
             iterator_key    => 'array',
             iterator_cols   => 'columns',
             current_col     => 'column',
             var_start       => undef,
             var_end         => undef,
             var_key         => 'var',
             self_close      => '/>',
             html            => \@HTML,
             available_refs  => \%available_refs
                   }, $class;
  return $self;
}

#┌ setHTML ────────────────────────────────────────────────────────────────┐
#│Sets the $self->{html} array reference.  The obvious uses are to set the │
#│the array reference if you did already do so with new(@HTML), or to re-  │
#│set this reference if you no longer want what you had previously put     │
#│there.                                                                   │
#└─────────────────────────────────────────────────────────────────────────┘
sub setHTML {
  my $self = shift;
  
  if (@_ > 1) {
    @{$self->{html}} = @_;
  } elsif (@_ > 0) {
    my $filename = shift;
    
    open(HTML, $filename) or die "Can't open $filename: $!";
    @{$self->{html}} = <HTML>;
    close(HTML);
  }
}

#┌ getHTML ────────────────────────────────────────────────────────────────┐
#│Returns an array of the contents of $self->{html} if it exists.  If      │
#│$self->{html} is undefined, then this subroutine returns undef.          │
#└─────────────────────────────────────────────────────────────────────────┘
sub getHTML {
  my $self = shift;
  
  if (@{$self->{html}}) {
    return @{$self->{html}};
  } else {
    return undef;
  }
}

#┌ parseHTML ──────────────────────────────────────────────────────────────┐
#│The way this subroutine is structured is the source of it's tremendous   │
#│speed as well as the reason for its unreadability.  It goes through each │
#│line of HTML and searches for the tags it will need to replace with data.│
#│No objects are created for this.  If there were, this section would be   │
#│a lot more readable and easier to maintain.  But it would also slow it   │
#│down.                                                                    │
#└─────────────────────────────────────────────────────────────────────────┘
sub parseHTML {
  my $self = shift;
  %{$self->{available_refs}} = @_;
  my $setup_found    = $self->{false};
  my $iterator_found = $self->{false};
  my $if_found       = $self->{false};
  my $unless_found   = $self->{false};
  my @parsed_contents;
  my ($iterator_array, $iterator_columns, @iterator_interior, $condition, @conditional_interior);
  
  #-> Be certain the $self->{html} reference is defined before parsing.
  if (@{$self->{html}}) {  
    foreach (@{$self->{html}}) {
      #┌───────────────────────────────────────────────────────────────────┐
      #│Step 1: Find the setup tag first.  Don't worry about any other tags│
      #│that MAY exists in the code preceding this tag.  BTW, the setup tag│
      #│should look like so:                                               │
      #│<%@ ts prefix="your_chosen_prefix" %>                              │
      #└───────────────────────────────────────────────────────────────────┘
      if ($setup_found eq $self->{false}) {
        if (/$self->{setup_tag_open}.*$self->{setup_tag_close}/) {
          $setup_found = $self->{true};
          $self->{prefix} = $self->getTagProperty($self->{prefix_atrib}, $_);
          $self->{iterator_start} = "<".$self->{prefix}.":".$self->{iterator_tag}." ";
          $self->{iterator_end}   = "</".$self->{prefix}.":".$self->{iterator_tag}.">";
          $self->{var_start}      = "<".$self->{prefix}.":var ";
          $self->{var_end}        = "</".$self->{prefix}.":var>";
          next; #This would be the reason why the setup tag needs to be on its own line.
        }
        push (@parsed_contents, $_);
        next; #Who cares about the rest of this loop if the setup tag hasn't been found yet?
      }
      
      #┌───────────────────────────────────────────────────────────────────┐
      #│ITERATOR                                                           │
      #│If $self->{iterator_start} is defined and the value thereof has    │
      #│been found, get ready to keep parsing until we find the closing    │
      #│iterator tag.                                                      │
      #└───────────────────────────────────────────────────────────────────┘
      if ($self->{iterator_start} && /$self->{iterator_start}/) {
        my $interior;
        $iterator_found   = $self->{true};
        $iterator_array   = $self->getTagProperty($self->{iterator_key}, $_);
        $iterator_columns = $self->getTagProperty($self->{iterator_cols}, $_);
        $interior = $_;
        if (/$self->{iterator_end}/) {
          $interior =~ s/.*$self->{iterator_start}$self->{iterator_key}=(?:"|')\w+(?:"|') *>(^$self->{iterator_end})*^$self->{iterator_end}.*/$1/;
                       s/(?:^$self->{iterator_end})*$self->{iterator_end}(.*)/$1/;
          push (@iterator_interior, $interior);
        } else {
          $interior =~ s/.*$self->{iterator_start}$self->{iterator_key}=(?:"|')\w+(?:"|')(?: *| $self->{iterator_cols}=(?:"|')\d+(?:"|') *)>(.*)/$1/;
                       s/(.*)$self->{iterator_start}.*/$1/s;
          push (@iterator_interior, $interior);
          push (@parsed_contents, $_);
          next;
        }      
      }

      #┌───────────────────────────────────────────────────────────────────┐
      #│ITERATOR                                                           │
      #│If the iterator start tag has been found, save this chunk as all   │
      #│belonging to this iterator.  When the ending itrerator tag has been│
      #│found, process the other tags that were found within these         │
      #│iterator tags.                                                     │
      #└───────────────────────────────────────────────────────────────────┘
      if ($iterator_found eq $self->{true}) {
        if (/$self->{iterator_end}/) {
          $iterator_found = $self->{false};
          s/(?:^$self->{iterator_end})*$self->{iterator_end}(.*)/$1/;
          
          my $string;
          
          foreach my $lemming (@iterator_interior) {
            $string .= $lemming;
          }
  
          my @processed_contents = $self->processIterator($iterator_array, $iterator_columns, @iterator_interior);
  
          @iterator_interior = '';
          push (@parsed_contents, @processed_contents);
          redo; #There COULD be ANOTHER iterator tag on this line.  If so, shall we not process it?
        } else {
          push (@iterator_interior, $_);
          next;
        }
      }

      #┌───────────────────────────────────────────────────────────────────┐
      #│STAND-ALONE TAGS                                                   │
      #│If a tag has been found outside of iterator tags, process it in-   │
      #│place.                                                             │
      #└───────────────────────────────────────────────────────────────────┘
      if ($self->{var_start} && $self->{var_end} && /$self->{var_start}/) {
        my @vars = $self->getTags($self->{var_start}, $self->{self_close}, $_);
        $_ = $self->processStandAlone($_, @vars);
      }
      
      push(@parsed_contents, $_);
    }
    
    @{$self->{html}} = @parsed_contents;
    
  } else {
    # nothing
  }
}

#┌ printHTML ──────────────────────────────────────────────────────────────┐
#│Prints the contents of $self->{html} (presumably through the CGI gateway)│
#└─────────────────────────────────────────────────────────────────────────┘
sub printHTML {
  my $self = shift;
  
  print "Content-Type: text/html\n\n";
  
  if (@{$self->{html}}) {
    foreach (@{$self->{html}}) {
      print $_;
    }
  } else {
    print 'There was no HTML to print.  Set the HTML via new(@HTML) or setHTML(@HTML)';
  }
}

#┌ getTags ────────────────────────────────────────────────────────────────┐
#│IMPLEMENTED INTERNALLY ONLY                                              │
#│Return all the tag "property names."  These are the names associated with│
#│scalars provided to us via parseHTML(%refs).                             │
#└─────────────────────────────────────────────────────────────────────────┘
sub getTags {
  my $self = shift;
  my ($tagStart, $tagEnd, $line) = @_;
  my @property_names;
  
  my @tags = ($line =~ /($tagStart(?:\w+)=(?:"|')\w+(?:"|')(?: *| \w+=(?:"|')\w+(?:"|') *)$tagEnd)/g);
  foreach my $tag (@tags) {
    my $propertyName = $tag;
    $propertyName =~ s/$tagStart(\w+)=(?:"|')\w+(?:"|')(?: *| \w+=(?:"|')\w+(?:"|') *)$tagEnd/$1/;
    push (@property_names, $self->getTagProperty($propertyName, $tag));
  }
  
  return @property_names;
}

#┌ getTagProperty ─────────────────────────────────────────────────────────┐
#│IMPLEMENTED INTERNALLY ONLY                                              │
#│Returns the value of an HTML tag property.                               │
#└─────────────────────────────────────────────────────────────────────────┘
sub getTagProperty {
  my $self = shift;
  my ($pName, $line) = @_;

  $line =~ s/.*$pName=(?:"|')(\w+)(?:"|').*/$1/s;
  
  return $line;  
}

#┌ processStandAlone ──────────────────────────────────────────────────────┐
#│IMPLEMENTED INTERNALLY ONLY                                              │
#│Do the redundant dirty-work or replacing scalar var tags in the HTML with│
#│the value of the scalars they represent.                                 │
#└─────────────────────────────────────────────────────────────────────────┘
sub processStandAlone {
  my $self = shift;
  my ($line, @vars) = @_;
  
  foreach my $varName (@vars) {
    if (${$self->{available_refs}{$varName}} && ref $self->{available_refs}{$varName} eq $self->{ref_scalar}) {
      $line =~ s/$self->{var_start}$self->{var_key}=(?:"|')$varName(?:"|') *$self->{self_close}/${$self->{available_refs}{$varName}}/;
    } else {
      $line =~ s/$self->{var_start}$self->{var_key}=(?:"|')$varName(?:"|') *$self->{self_close}//;
    }
  }
  
  return $line;
}

#┌ processIterator ────────────────────────────────────────────────────────┐
#│IMPLEMENTED INTERNALLY ONLY                                              │
#│Do the redundant dirty-work or replacing scalar var tags in the HTML with│
#│the value of the scalars they represent.                                 │
#└─────────────────────────────────────────────────────────────────────────┘
sub processIterator {
  my $self = shift;
  my ($arrayName, $columns, @iterator_snippet) = @_;
  my (%usedVars, @newContents, $currentCol);
  
  if (ref $self->{available_refs}{$arrayName} eq $self->{ref_array}) {
    #foreach my $element (@{$self->{available_refs}{$arrayName}}) {
    for (my $i = 0; $i < @{$self->{available_refs}{$arrayName}}; $i++) {
      my $element = $self->{available_refs}{$arrayName}[$i];
      if ($columns) {
          $currentCol = 1;
          %usedVars = ();
      }
      
      foreach my $line (@iterator_snippet) {
        if ($line =~ $self->{var_start}) {
          my $newline = $line;
          my @vars = $self->getTags($self->{var_start}, $self->{self_close}, $newline);
          
          foreach my $varName (@vars) {
            if ($columns) {
              if ($newline =~ /$self->{current_col}=(?:"|')(\d+)(?:"|')/) {
                if ($1 > $currentCol) {
                  $currentCol > $columns ? last : $currentCol++;
                  $element = $self->{available_refs}{$arrayName}[++$i];
                }
              }
              
              SWITCH : {
                if ($varName eq $arrayName) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|')(?: *| $self->{current_col}=(?:"|')\d+(?:"|') *)$self->{self_close}/$element/;
                    last SWITCH; }
                if (ref $self->{available_refs}{$varName} eq $self->{ref_hash}) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|')(?: *| $self->{current_col}=(?:"|')\d+(?:"|') *)$self->{self_close}/$self->{available_refs}{$varName}{$element}/;
                    last SWITCH; }
                if (ref $self->{available_refs}{$varName} eq $self->{ref_scalar}) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|')(?: *| $self->{current_col}=(?:"|')\d+(?:"|') *)$self->{self_close}/${$self->{available_refs}{$varName}}/;
                    last SWITCH; }
                $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|')(?: *| $self->{current_col}=(?:"|')\d+(?:"|') *)$self->{self_close}//;
              } # SWITCH
            } else {
              SWITCH : {
                if ($varName eq $arrayName) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|') *$self->{self_close}/$element/;
                    last SWITCH; }
                if (ref $self->{available_refs}{$varName} eq $self->{ref_hash}) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|') *$self->{self_close}/$self->{available_refs}{$varName}{$element}/;
                    last SWITCH; }
                if (ref $self->{available_refs}{$varName} eq $self->{ref_scalar}) {
                    $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|') *$self->{self_close}/${$self->{available_refs}{$varName}}/;
                    last SWITCH; }
                $newline =~ s/$self->{var_start}(?:\w+)=(?:"|')$varName(?:"|') *$self->{self_close}//;
              } # SWITCH
            }
          }
          push (@newContents, $newline);
        } else {
          push (@newContents, $line);
        }
      }
    }
  }
  
  return @newContents;
}

#┌ printLog ───────────────────────────────────────────────────────────────┐
#│IMPLEMENTED INTERNALLY ONLY                                              │
#│Usage: printLog(string) NOT $self->printLog(string)                      │
#│For debugging by the author only.                                        │
#└─────────────────────────────────────────────────────────────────────────┘
sub printLog {
  my $debug = "debug.log";  
  my $msg  = shift;
  
  open (LOG, ">>$debug");
  print LOG "$msg\n";
  close(LOG);
}

1;
