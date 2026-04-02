#!/usr/bin/perl
#┌──────────────────────────────────────────► INFO ◄───────────────────────────────────────────┐
#│ THE COMMENTS ON THIS PAGE ARE UTF-8 (unicode) ENCODED.                                      │
#│ You may wish to change your terminal client settings or your IDE settings to prevent        │
#│ jibberish from apearing on your screen.                                                     │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
use lib qw(./ ./ManagerLib);

use CGI;
use File::Copy;
use FileParser;
use HTML::Entities;
use strict;

#                                  ╔══════════════╗
#╔═════════════════════════════════╣ FILE MANAGER ╠═════════════════════════════════╗
#║                                 ╚══════════════╝                                 ║
#║                                                                                  ║
#╚══════════════════════════════════════════════════════════════════════════════════╝

my $cgi = new CGI;

my $FILE_MANAGER_CONFIG = "filemanager.conf";
my %config = getConfig($FILE_MANAGER_CONFIG);
my @critical_vars; #If any of these are missing, File Manager will puke.

my $debug_log       = "/var/www/html/filemanager/debug.log";

#► User-defined variables ──────────────────────────────────────────────────────────────────────
my $DEFAULT_DIR     = "/var/www/html";
my $FILE_MANAGER_ROOT = $config{'tsroot'}; push (@critical_vars, \$FILE_MANAGER_ROOT);

#► Truth ───────────────────────────────────────────────────────────────────────────────────────
my $TRUE = 1; my $FALSE = 0; my $UNKNOWN = -1;

#► File object setup ───────────────────────────────────────────────────────────────────────────
my $FILE = 1; my $DIRECTORY = 2;

#► File type setup ─────────────────────────────────────────────────────────────────────────────
my $FILE_TEXT = 1; my $FILE_BIN = 2;

#► Actions ─────────────────────────────────────────────────────────────────────────────────────
my $GET_DIR_LISTING   = 'dir_listing';
my $GET_SEARCH_RESULT = 'search';
my $GET_COMMAND_PAGE  = 'shell';
my $GET_COMMAND       = 'executeCommand';
my $CHANGE_MODE       = 'chmod';
my $CHANGE_MODE_FINAL = 'confirmChmod';
my $GET_CRT_FILE_PAGE = 'newFile';
my $GET_CRT_DIR_PAGE  = 'newDir';
my $CREATE_FILE       = 'createFile';
my $CREATE_DIR        = 'createDir';
my $UPLOAD_FILES      = 'uploadFiles';
my $UPLOAD_FILES_CNF  = 'finalizeUploadFiles';
my $RENAME_DIR        = 'renameDir';
my $RENAME_DIR_CNF1   = 'confirmRenameDir1';
my $RENAME_DIR_CNF2   = 'confirmRenameDir2';
my $RENAME_DIR_FINAL  = 'finalizeRenameDir';
my $DELETE_DIR        = 'deleteDir';
my $DELETE_DIR_CNF    = 'confirmDeleteDir';
my $DELETE_DIR_FINAL  = 'finalizeDeleteDir';
my $VIEW_FILE         = 'viewFile';
my $EDIT_FILE         = 'editFile';
my $EDIT_FILE_FINAL   = 'finalizeEditFile';
my $COPY_FILE         = 'copyFile';
my $DOWNLOAD_FILE         = 'downloadFile';
my $COPY_FILE_CNF     = 'confirmCopyFile';
my $COPY_FILE_FINAL   = 'finalizeCopyFile';
my $MOVE_FILE         = 'moveFile';
my $MOVE_FILE_CNF     = 'confirmMoveFile';
my $MOVE_FILE_FINAL   = 'finalizeMoveFile';
my $RENAME_FILE       = 'renameFile';
my $RENAME_FILE_CNF   = 'confirmRenameFile';
my $RENAME_FILE_FINAL = 'finalizeRenameFile';
my $DELETE_FILE       = 'deleteFile';
my $DELETE_FILE_FINAL = 'finalizeDeleteFile';

#► HTML pages ──────────────────────────────────────────────────────────────────────────────────
my $HTML_ERROR         = "$FILE_MANAGER_ROOT$config{'skin'}/Error.html";
                         push (@critical_vars, \$HTML_ERROR);
my $HTML_LIST          = "$FILE_MANAGER_ROOT$config{'skin'}/List.html";
                         push (@critical_vars, \$HTML_LIST);
my $HTML_SEARCH_RES_F  = "$FILE_MANAGER_ROOT$config{'skin'}/FileSearchResult.html";
                         push (@critical_vars, \$HTML_SEARCH_RES_F);
my $HTML_SEARCH_RES_D  = "$FILE_MANAGER_ROOT$config{'skin'}/DirSearchResult.html";
                         push (@critical_vars, \$HTML_SEARCH_RES_D);
my $HTML_SEARCH_FAILED = "$FILE_MANAGER_ROOT$config{'skin'}/SearchFailed.html";
                         push (@critical_vars, \$HTML_SEARCH_FAILED);
my $HTML_SHELL_COMMAND = "$FILE_MANAGER_ROOT$config{'skin'}/ShellCommand.html";
                         push (@critical_vars, \$HTML_SHELL_COMMAND);
my $HTML_CHMOD         = "$FILE_MANAGER_ROOT$config{'skin'}/ChangeMode.html";
                         push (@critical_vars, \$HTML_CHMOD);
my $HTML_CREATE_FILE   = "$FILE_MANAGER_ROOT$config{'skin'}/CreateFile.html";
                         push (@critical_vars, \$HTML_CREATE_FILE);
my $HTML_CREATE_DIR    = "$FILE_MANAGER_ROOT$config{'skin'}/CreateDir.html";
                         push (@critical_vars, \$HTML_CREATE_DIR);
my $HTML_UPLOAD_FILES  = "$FILE_MANAGER_ROOT$config{'skin'}/UploadFiles.html";
                         push (@critical_vars, \$HTML_UPLOAD_FILES);
my $HTML_RENAME_DIR    = "$FILE_MANAGER_ROOT$config{'skin'}/RenameDir.html";
                         push (@critical_vars, \$HTML_RENAME_DIR);
my $HTML_RENAME_DIR_C1 = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmRenameDir1.html";
                         push (@critical_vars, \$HTML_RENAME_DIR_C1);
my $HTML_RENAME_DIR_C2 = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmRenameDir2.html";
                         push (@critical_vars, \$HTML_RENAME_DIR_C2);
my $HTML_RENAME_DIR_S  = "$FILE_MANAGER_ROOT$config{'skin'}/RenameDirSuccess.html";
                         push (@critical_vars, \$HTML_RENAME_DIR_S);
my $HTML_DELETE_DIR_C1 = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmDeleteDir1.html";
                         push (@critical_vars, \$HTML_DELETE_DIR_C1);
my $HTML_DELETE_DIR_C2 = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmDeleteDir2.html";
                         push (@critical_vars, \$HTML_DELETE_DIR_C2);
my $HTML_DELETE_DIR_S  = "$FILE_MANAGER_ROOT$config{'skin'}/DeleteDirSuccess.html";
                         push (@critical_vars, \$HTML_DELETE_DIR_S);
my $HTML_VIEW_FILE     = "$FILE_MANAGER_ROOT$config{'skin'}/ViewTextFile.html";
                         push (@critical_vars, \$HTML_VIEW_FILE);
my $HTML_EDIT_FILE     = "$FILE_MANAGER_ROOT$config{'skin'}/EditTextFile.html";
                         push (@critical_vars, \$HTML_EDIT_FILE);
my $HTML_EDIT_FILE_S   = "$FILE_MANAGER_ROOT$config{'skin'}/EditTextFileSuccess.html";
                         push (@critical_vars, \$HTML_EDIT_FILE_S);
my $HTML_COPY_FILE     = "$FILE_MANAGER_ROOT$config{'skin'}/CopyFile.html";
                         push (@critical_vars, \$HTML_COPY_FILE);
my $HTML_COPY_FILE_C   = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmCopyFile.html";
                         push (@critical_vars, \$HTML_COPY_FILE_C);
my $HTML_COPY_FILE_S   = "$FILE_MANAGER_ROOT$config{'skin'}/CopyFileSuccess.html";
                         push (@critical_vars, \$HTML_COPY_FILE_S);
my $HTML_MOVE_FILE     = "$FILE_MANAGER_ROOT$config{'skin'}/MoveFile.html";
                         push (@critical_vars, \$HTML_MOVE_FILE);
my $HTML_MOVE_FILE_C   = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmMoveFile.html";
                         push (@critical_vars, \$HTML_MOVE_FILE_C);
my $HTML_MOVE_FILE_S   = "$FILE_MANAGER_ROOT$config{'skin'}/MoveFileSuccess.html";
                         push (@critical_vars, \$HTML_MOVE_FILE_S);
my $HTML_RENAME_FILE   = "$FILE_MANAGER_ROOT$config{'skin'}/RenameFile.html";
                         push (@critical_vars, \$HTML_RENAME_FILE);
my $HTML_RENAME_C      = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmRenameFile.html";
                         push (@critical_vars, \$HTML_RENAME_C);
my $HTML_RENAME_S      = "$FILE_MANAGER_ROOT$config{'skin'}/RenameFileSuccess.html";
                         push (@critical_vars, \$HTML_RENAME_S);
my $HTML_DELETE_FL_C   = "$FILE_MANAGER_ROOT$config{'skin'}/ConfirmDeleteFile.html";
                         push (@critical_vars, \$HTML_DELETE_FL_C);
my $HTML_DELETE_FL_S   = "$FILE_MANAGER_ROOT$config{'skin'}/DeleteFileSuccess.html";
                         push (@critical_vars, \$HTML_DELETE_FL_S);

#► HTML tag setup ──────────────────────────────────────────────────────────────────────────────
my $HTML_VAR_HOLDER   = '@@html_var@@';
my $HTML_HREF_HOLDER  = '@@html_href@@';
my $HTML_HREF         = '<a href="'.$HTML_HREF_HOLDER.'">'.$HTML_VAR_HOLDER.'</a>';

#► Other global variables ──────────────────────────────────────────────────────────────────────
my (%foType, %foMode, %foUID, %foGID, %foSize, %foLMDate, %fileSearch, %foViewable, %foEditable, %curDirPath, %curDirName);
my (@foKeys, @dirNameKeys, %escaped, @curDirKeys, @fileNameKeys, @searchResults, @f_upload);
my ($up_one_dir, $command_result, $error_msg, $old_file_name, $new_file_name,
    $old_dir_name, $new_dir_name, $current_file, $file_contents);

#► CGI variables ───────────────────────────────────────────────────────────────────────────────
my $action        = $cgi->param('action');
my $searchFile    = $cgi->param('searchFile');
my $searchDir     = $cgi->param('searchDir');
my $shellCmd      = $cgi->param('shellCommand');
my $fileObject    = $cgi->param('fileObject');
my $modor         = $cgi->param('or');
my $modow         = $cgi->param('ow');
my $modox         = $cgi->param('ox');
my $modgr         = $cgi->param('gr');
my $modgw         = $cgi->param('gw');
my $modgx         = $cgi->param('gx');
my $modar         = $cgi->param('ar');
my $modaw         = $cgi->param('aw');
my $modax         = $cgi->param('ax');
my $new_file      = $cgi->param('newFile');
my $new_dir       = $cgi->param('newDir');
   $f_upload[0]   = $cgi->param('file01');
   $f_upload[1]   = $cgi->param('file02');
   $f_upload[2]   = $cgi->param('file03');
   $f_upload[3]   = $cgi->param('file04');
   $f_upload[4]   = $cgi->param('file05');
   $f_upload[5]   = $cgi->param('file06');
   $f_upload[6]   = $cgi->param('file07');
   $f_upload[7]   = $cgi->param('file08');
   $f_upload[8]   = $cgi->param('file09');
   $f_upload[9]   = $cgi->param('file10');
   $f_upload[10]   = $cgi->param('file11');
   $f_upload[11]   = $cgi->param('file12');
   $f_upload[12]   = $cgi->param('file13');
   $f_upload[13]   = $cgi->param('file14');
   $f_upload[14]   = $cgi->param('file15');
   $old_dir_name  = $cgi->param('oldDirName');
   $new_dir_name  = $cgi->param('newDirName');
   $current_file  = $cgi->param('currentFile');
my $fileName	  = $cgi->param('fileName');
   $old_file_name = $cgi->param('oldFileName');
   $new_file_name = $cgi->param('newFileName');
my $new_file_cnts = $cgi->param('newFileContents');
my $current_path  = $cgi->param('path');
   $current_path  = $DEFAULT_DIR unless $current_path;
   $current_path  = $cgi->unescape($current_path) if $current_path;
   $current_path .= '/' unless index($current_path, '/', (length($current_path) -1)) > -1;
   populateCurDir();

   if ($current_path ne $DEFAULT_DIR) {
     $up_one_dir = chopLastDir($current_path);
   } else {
     $up_one_dir = $current_path;
   }

#► Variables to pass to the HTML Parser ────────────────────────────────────────────────────────
  my %HTMLParseVar = (
  'currentPath'    =>  \$current_path,
  'up_one_dir'     =>  \$up_one_dir,
  'command_result' =>  \$command_result,
  'error_msg'      =>  \$error_msg,
  'fileObject'     =>  \$fileObject,
  'or'             =>  \$modor,
  'ow'             =>  \$modow,
  'ox'             =>  \$modox,
  'gr'             =>  \$modgr,
  'gw'             =>  \$modgw,
  'gx'             =>  \$modgx,
  'ar'             =>  \$modar,
  'aw'             =>  \$modaw,
  'ax'             =>  \$modax,
  'oldFileName'    =>  \$old_file_name,
  'newFileName'    =>  \$new_file_name,
  'oldDirName'     =>  \$old_dir_name,
  'newDirName'     =>  \$new_dir_name,
  'currentFile'    =>  \$current_file,
  'fileContents'   =>  \$file_contents,
  'foKeys'         =>  \@foKeys,
  'dirKeys'        =>  \@dirNameKeys,
  'escaped'        =>  \%escaped,
  'curDirKeys'     =>  \@curDirKeys,
  'fileKeys'       =>  \@fileNameKeys,
  'searchResults'  =>  \@searchResults,
  'foType'         =>  \%foType,
  'foMode'         =>  \%foMode,
  'foUID'          =>  \%foUID,
  'foGID'          =>  \%foGID,
  'foSize'         =>  \%foSize,
  'foLMDate'       =>  \%foLMDate,
  'foViewable'     =>  \%foViewable,
  'foEditable'     =>  \%foViewable,
  'fileSearch'     =>  \%fileSearch,
  'currentDir'     =>  \%curDirPath,
  'currentDirName' =>  \%curDirName,
  'host'		   =>  \$ENV{'HTTP_HOST'},
                     );

checkCriticalVars(@critical_vars);

# HANDLE ACTION:
# Decide how to proceed based on what was the CGI variable 'action'
SWITCH: {
    if (!$action)                      { getFileListing();    last SWITCH; }
    if ($action eq $GET_DIR_LISTING)   { getFileListing();    last SWITCH; }
    if ($action eq $GET_SEARCH_RESULT) { getSearchResult();   last SWITCH; }
    if ($action eq $GET_COMMAND_PAGE)  { getCommandPage();    last SWITCH; }
    if ($action eq $GET_COMMAND)       { getCommand();        last SWITCH; }
    if ($action eq $CHANGE_MODE)       { changeMode();        last SWITCH; }
    if ($action eq $CHANGE_MODE_FINAL) { changeModeFinal();   last SWITCH; }
    if ($action eq $GET_CRT_FILE_PAGE) { getCreateFilePage(); last SWITCH; }
    if ($action eq $GET_CRT_DIR_PAGE)  { getCreateDirPage();  last SWITCH; }
    if ($action eq $CREATE_FILE)       { createFile();        last SWITCH; }
    if ($action eq $CREATE_DIR)        { createDir();         last SWITCH; }
    if ($action eq $UPLOAD_FILES)      { uploadFiles();       last SWITCH; }
    if ($action eq $UPLOAD_FILES_CNF)  { uploadFilesFinal();  last SWITCH; }
    if ($action eq $RENAME_DIR)        { renameDir();         last SWITCH; }
    if ($action eq $RENAME_DIR_CNF1)   { renameDirConfirm1(); last SWITCH; }
    if ($action eq $RENAME_DIR_CNF2)   { renameDirConfirm2(); last SWITCH; }
    if ($action eq $RENAME_DIR_FINAL)  { renameDirFinal();    last SWITCH; }
    if ($action eq $DELETE_DIR)        { deleteDir();         last SWITCH; }
    if ($action eq $DELETE_DIR_CNF)    { deleteDirConfirm();  last SWITCH; }
    if ($action eq $DELETE_DIR_FINAL)  { deleteDirFinal();    last SWITCH; }
    if ($action eq $VIEW_FILE)         { viewFile();          last SWITCH; }
    if ($action eq $EDIT_FILE)         { editFile();          last SWITCH; }
    if ($action eq $EDIT_FILE_FINAL)   { editFileFinal();     last SWITCH; }
    if ($action eq $DOWNLOAD_FILE)         { downloadFile();          last SWITCH; }
    if ($action eq $COPY_FILE)         { copyFile();          last SWITCH; }
    if ($action eq $COPY_FILE_CNF)     { copyFileConfirm();   last SWITCH; }
    if ($action eq $COPY_FILE_FINAL)   { copyFileFinal();     last SWITCH; }
    if ($action eq $MOVE_FILE)         { moveFile();          last SWITCH; }
    if ($action eq $MOVE_FILE_CNF)     { moveFileConfirm();   last SWITCH; }
    if ($action eq $MOVE_FILE_FINAL)   { moveFileFinal();     last SWITCH; }
    if ($action eq $RENAME_FILE)       { renameFile();        last SWITCH; }
    if ($action eq $RENAME_FILE_CNF)   { renameFileConfirm(); last SWITCH; }
    if ($action eq $RENAME_FILE_FINAL) { renameFileFinal();   last SWITCH; }
    if ($action eq $DELETE_FILE)       { deleteFile();        last SWITCH; }
    if ($action eq $DELETE_FILE_FINAL) { deleteFileFinal();   last SWITCH; }

    returnError("ERROR: The request you sent does not match with what I am built to do.");
} # end SWITCH

exit();

#┌ returnError ────────────────────────────────────────────────────────────────────────────────┐
#│ Return an error HTML page.                                                                  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub returnError {
  $error_msg = shift;

  my $HTML = FileParser->new($HTML_ERROR);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
  exit(0);
}

#┌ getFileListing ─────────────────────────────────────────────────────────────────────────────┐
#│ Create and display a file listing.                                                          │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getFileListing {
  getLSListing();
  return;

  while (my $fo = <$current_path/*>) {
    my @foAttributes = getFileObjectAttributes($fo);

    next unless @foAttributes;

    unless ($foAttributes[0] == $UNKNOWN) {
      my $foName = $foAttributes[1];

      $foType{$foName}   = $foAttributes[0];
      $foMode{$foName}   = $foAttributes[2];
      $foUID{$foName}    = $foAttributes[3];
      $foGID{$foName}    = $foAttributes[4];
      $foSize{$foName}   = $foAttributes[5];
      $foLMDate{$foName} = $foAttributes[6];

      push (@dirNameKeys,  $foName) if $foType{$foName} == $DIRECTORY;
      push (@fileNameKeys, $foName) if $foType{$foName} == $FILE;
      push (@foKeys, $foName);
    }
  }

  my $HTML = FileParser->new($HTML_LIST);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
} # end getFileListing

#┌ getLSListing ───────────────────────────────────────────────────────────────────────────────┐
#│ Use the *NIX "ls" system command to get a file listing.  Obviously, this is OS-dependant    │
#│ and as such, will be replace in a future release.                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getLSListing {
  getLSList($current_path);

  my $HTML = FileParser->new($HTML_LIST);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}


#┌ getSearchResult ────────────────────────────────────────────────────────────────────────────┐
#│ Simply written code that generates correct results, and does so much more quickly then before
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getSearchResult {
	my @foldersNFiles;
	if ( $searchDir ) {
		@searchResults = `find / 2>/dev/null -type d -iname "$searchDir"`;
	} elsif ( $searchFile ) {
		@foldersNFiles = `find / 2>/dev/null -type f -iname "$searchFile" -printf '\%h\n\%f\n'`;
	}
	# gotta hide in a "<!--$i-->" into each key because otherwise it (the MVC framework and this function) doesn't support multiple matches in the same directory
	for ( my $i = 0; $i < scalar(@foldersNFiles)-1; $i += 2 ) {
		chomp( $foldersNFiles[$i] );
		chomp( $foldersNFiles[$i+1] );
		push( @searchResults, "<!--$i-->" . $foldersNFiles[$i] );
		$fileSearch{"<!--$i-->" . $foldersNFiles[$i]} = $foldersNFiles[$i+1];
	}
	if (@searchResults && $searchFile) {
		my $HTML = FileParser->new($HTML_SEARCH_RES_F);
		$HTML->parseHTML(%HTMLParseVar);
		$HTML->printHTML();
	} elsif (@searchResults && $searchDir) {
		my $HTML = FileParser->new($HTML_SEARCH_RES_D);
		$HTML->parseHTML(%HTMLParseVar);
		$HTML->printHTML();
	} else {
		my $HTML = FileParser->new($HTML_SEARCH_FAILED);
		$HTML->parseHTML(%HTMLParseVar);
		$HTML->printHTML();
	}
}

#┌ getSearchResult ────────────────────────────────────────────────────────────────────────────┐
#│ Poorly written code that generates strange, incorrect results.                              │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
#sub getSearchResult {
#  my @dirs = ("");
#  my @temp_html;
#  my ($cleanDir, $fileQualifies);
#
#  if ($searchFile) {
#    foreach my $dir (@dirs) {
#      while (my $fo = <$dir/*>) {
#        my ($isLink, $isDir, $isFile);
#        my $currentFile = $fo;
#           $currentFile =~ s#.*/##;
#        $isLink = 1 if -l $fo;
#        $isDir  = 1 if -d $fo;
#        $isFile = 1 if -f $fo;
#
#        if ($isFile && $fo =~ /$searchFile/i) {
#          push (@searchResults, $dir);
#          $fileSearch{$dir} = $currentFile;
#        }
#
#        $cleanDir = 1 if $isDir;
#        $cleanDir = 0 if $isLink;
#        if ($cleanDir == 1) { push (@dirs, $fo); }
#      }
#    }
#  } elsif ($searchDir) {
#    foreach my $dir (@dirs) {
#      while (my $fo = <$dir/*>) {
#        my $currentDir = $fo;
#           $currentDir =~ s#.*/##;
#
#        if (-d $fo && $currentDir =~ /$searchDir/i) {
#          push (@searchResults, $fo) unless -l $fo;
#        }
#
#        $cleanDir = 1 if -d $fo;
#        $cleanDir = 0 if -l $fo;
#        if ($cleanDir == 1) { push (@dirs, $fo); }
#      }
#    }
#  }
#
#  if (@searchResults && $searchFile) {
#    my $HTML = FileParser->new($HTML_SEARCH_RES_F);
#    $HTML->parseHTML(%HTMLParseVar);
#    $HTML->printHTML();
#  } elsif (@searchResults && $searchDir) {
#    my $HTML = FileParser->new($HTML_SEARCH_RES_D);
#    $HTML->parseHTML(%HTMLParseVar);
#    $HTML->printHTML();
#  } else {
#    my $HTML = FileParser->new($HTML_SEARCH_FAILED);
#    $HTML->parseHTML(%HTMLParseVar);
#    $HTML->printHTML();
#  }
#} # end getSearchResult

#┌ getCommandPage ─────────────────────────────────────────────────────────────────────────────┐
#│ Return the "Shell Command" page.                                                            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getCommandPage {
  my $HTML = FileParser->new($HTML_SHELL_COMMAND);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ getCommand ─────────────────────────────────────────────────────────────────────────────────┐
#│ Return the "Shell Command" page (with the command result).                                  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getCommand {
  $current_path =~ s/'/\\'/g;
  $command_result = `cd '$current_path' && $shellCmd`;
  my $HTML = FileParser->new($HTML_SHELL_COMMAND);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ getCreateFilePage ──────────────────────────────────────────────────────────────────────────┐
#│ Return the Create File page.                                                                │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getCreateFilePage {
  my $HTML = FileParser->new($HTML_CREATE_FILE);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ getCreateDirPage ───────────────────────────────────────────────────────────────────────────┐
#│ Return the Create Directory page.                                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getCreateDirPage {
  my $HTML = FileParser->new($HTML_CREATE_DIR);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ createFile ─────────────────────────────────────────────────────────────────────────────────┐
#│ Create a file.  Return an error if unsuccessful or the dir listing page if successful.      │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub createFile {
  die "new_file does not exist" unless $new_file;
  my $possible_file = $current_path.$new_file;
  my $error;

  unless (-f $possible_file) {
    open (FILE, ">$possible_file") or $error = $!;
    close (FILE);
  }

  if ($error) {
    returnError("Couldn't create the file $possible_file: ".$error);
  } else {
    getFileListing();
  }
}

#┌ createDir ──────────────────────────────────────────────────────────────────────────────────┐
#│ Create a dirctory.  Return an error if unsuccessful or the dir listing page if successful.  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub createDir {
  my $possible_dir = $current_path.$new_dir;
  my $error;

  unless (-d $possible_dir) {
    mkdir($possible_dir) or $error = $!;
    close (FILE);
  }

  if ($error) {
    returnError("Couldn't create the file: ".$error);
  } else {
    getFileListing();
  }
}

#┌ uploadFiles ────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Upload Files page.                                                               │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub uploadFiles {
  my $HTML = FileParser->new($HTML_UPLOAD_FILES);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ uploadFilesFinal ───────────────────────────────────────────────────────────────────────────┐
#│ Upload the files.  Return an error if unsuccessful or the file list page if successful.     │
#│ Internet Explorer on the PC will send the filename AND the path from the remote machine     │
#│ (the coputer from which the file was uploaded).  All other browsers and IE on the Mac only  │
#│ send the file name.                                                                         │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub uploadFilesFinal {
  my $error;

  foreach my $file (@f_upload) {
    if ($file) {
      my $filename;

      if ($file =~ /(\\|\/)/) {
        $file  =~ m/^.*(\\|\/)(.*)/; # strip the remote path and keep the filename for PC IE
        $filename = $2;
      } else {
        $filename = $file;
      }

      $filename =~ s/\s/_/g;
      $filename = $current_path.$filename;

      open(NEWFILE, ">$filename") or $error = $!;
      while (<$file>) {
        print NEWFILE $_;
      }
      close (NEWFILE);
    }
  }

  if ($error) {
    returnError("Couldn't save the files in $current_path - ".$error);
  } else {
    getFileListing();
  }
}

#┌ renameDir ──────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Rename Directory page.                                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameDir {
  my $HTML = FileParser->new($HTML_RENAME_DIR);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ renameDirConfirm1 ──────────────────────────────────────────────────────────────────────────┐
#│ Return the first Directory Rename Confirm page.                                             │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameDirConfirm1 {
  my $compound_dir;

  my $compoud_dir = $old_dir_name;
  $compoud_dir =~ s /(.*\/).*/$1/;

  $new_dir_name = $compoud_dir."$new_dir_name";

  my $HTML = FileParser->new($HTML_RENAME_DIR_C1);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ renameDirConfirm2 ──────────────────────────────────────────────────────────────────────────┐
#│ Return the second Directory Rename Confirm page.                                            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameDirConfirm2 {
  my $HTML = FileParser->new($HTML_RENAME_DIR_C2);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ renameDirFinal ─────────────────────────────────────────────────────────────────────────────┐
#│ Rename the directory.  Return an error if unsuccessful or the success page if successful.   │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameDirFinal {
  my $error;

  rename ($old_dir_name, $new_dir_name) or $error = "Cannot rename directory! $!";

  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_RENAME_DIR_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ deleteDir ──────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Delete Directory page.                                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub deleteDir {
  my $HTML = FileParser->new($HTML_DELETE_DIR_C1);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ deleteDirConfirm ───────────────────────────────────────────────────────────────────────────┐
#│ Return the Delete Directory Confirm page.                                                   │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub deleteDirConfirm {
  my $HTML = FileParser->new($HTML_DELETE_DIR_C2);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ deleteDirFinal ─────────────────────────────────────────────────────────────────────────────┐
#│ Delete the directory.  Return an error if unsuccessful or a success page if successful.     │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub deleteDirFinal {
  my $error;

  recursivelyDelete($old_dir_name);

  my $HTML = FileParser->new($HTML_DELETE_DIR_S);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ recursivelyDelete ──────────────────────────────────────────────────────────────────────────┐
#│ recursively deleta a directory                                                              │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
## Rewritten June 20 2008 by Chris Michaelis to make it handle spaces
## in directory and file names
sub recursivelyDelete {
  my $file = shift;
  if (-d $file) {
	local *DIR;
	opendir DIR, $file or returnError("$file: $!");
	for (readdir DIR) {
        next if /^\.{1,2}$/;
        my $path = "$file/$_";
		unlink $path if -f $path;
		recursivelyDelete($path) if -d $path;
	}
	closedir DIR;
	rmdir $file or returnError("$file: $!");
  } else {
    unlink($file) or returnError("Couldn't unlink $file: $!");
  }
}

#┌ viewFile ───────────────────────────────────────────────────────────────────────────────────┐
#│ Display the file if it is a filetype we can view.                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub viewFile {
  my ($viewable, $fileType, $error) = getViewable($current_file);
  $file_contents = "";

  if ($error) {
    returnError($error);
  } elsif ($fileType eq "txt") {
    open (TEXTFILE, $current_file) or $error = "Can't open file! $!";
    foreach (<TEXTFILE>) {
      $file_contents .= $_;
    }
    close (TEXTFILE);
    $file_contents = encode_entities($file_contents);

    my $HTML = FileParser->new($HTML_VIEW_FILE);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  } elsif ($viewable == 1) {
    SWITCH: {
        if ($fileType eq "gif")  { print $cgi->header('image/gif'); last SWITCH; }
        if ($fileType eq "jpeg") { print $cgi->header('image/jpeg'); last SWITCH; }
        if ($fileType eq "png")  { print $cgi->header('image/png'); last SWITCH; }
        if ($fileType eq "swf")  { print $cgi->header('application/x-shockwave-flash'); last SWITCH; }
    }

    open (BINFILE, $current_file);
    while (<BINFILE>) {
      print $_;
    }
  } else {
    returnError("Can't view binary file of this file type.");
  }
}

#┌ editFile ───────────────────────────────────────────────────────────────────────────────────┐
#│ Allow the user to edit the file if it is a filetype we can edit.                            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub editFile {
  my ($editable, $error) = getEditable($current_file);
  $file_contents = "";

  if ($error) {
    returnError($error);
  } elsif ((stat $current_file)[4] == 0) {
    returnError("You do not have permissions sufficient to modify this file");
  } else {
    open (EDITABLE, $current_file);
    while (<EDITABLE>) {
      $file_contents .= $_;
    }
    close (EDITABLE);
    $file_contents  = escapeHTML($file_contents);

    my $HTML = FileParser->new($HTML_EDIT_FILE);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ editFileFinal ──────────────────────────────────────────────────────────────────────────────┐
#│ Commit the changes the user made to a file.  Return an error if unsuccessful, a success     │
#│ page if successful.                                                                         │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub editFileFinal {
  my $error;

  $new_file_cnts = unescapeHTML($new_file_cnts);
  $new_file_cnts =~ s/\r//g;

  open (EDITED, ">$current_file") or $error = "Can't open $current_file! $!";
  print EDITED $new_file_cnts;
  close (EDITED);

  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_EDIT_FILE_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ changeMode ─────────────────────────────────────────────────────────────────────────────────┐
#│ Prepare to change the file mode on a file or directory.  Notify the user if they do not     │
#│ have permission to do so.                                                                   │
#│ Setp 1: Get the current file mode of the file object the user wishes to modify.             │
#│ Step 2: Perform a CHMOD on the file object using the current file mode and check if the     │
#│         operation was successful.  If not, let the user know.  If it was, allow the user to │
#│         change the file mode on this file object.                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub changeMode {
  unless ($fileObject) {
    returnError("No file or directory was provided.  Can't change permissions on nothing!");
  }

  my $octmod = (stat $fileObject)[2];
  my $mod    = sprintf "%o", $octmod;
  my $MODR   = 'read';
  my $MODW   = 'write';
  my $MODX   = 'execute';
  my %modo = (
      mode => substr($mod, -3, 1),
     $MODR => \$modor,
     $MODW => \$modow,
     $MODX => \$modox
             );
  my %modg = (
     mode  => substr($mod, -2, 1),
     $MODR => \$modgr,
     $MODW => \$modgw,
     $MODX => \$modgx
             );
  my %moda = (
     mode  => substr($mod, -1, 1),
     $MODR => \$modar,
     $MODW => \$modaw,
     $MODX => \$modax
             );
  my @modeSlices = (\%modo, \%modg, \%moda);


  if ((chmod $octmod, $fileObject) < 1 || (stat $fileObject)[4] == 0) {
    returnError("You do not have permissions suficient to modify $fileObject");
  } else {
    my $markup = 'checked="checked"';

    foreach my $hashref (@modeSlices) {
      if ($$hashref{'mode'} == 4) { ${$$hashref{$MODR}} = $markup; }
      if ($$hashref{'mode'} == 2) { ${$$hashref{$MODW}} = $markup; }
      if ($$hashref{'mode'} == 1) { ${$$hashref{$MODX}} = $markup; }
      if ($$hashref{'mode'} == 7) { ${$$hashref{$MODR}} = $markup; ${$$hashref{$MODW}} = $markup; ${$$hashref{$MODX}} = $markup; }
      if ($$hashref{'mode'} == 6) { ${$$hashref{$MODR}} = $markup; ${$$hashref{$MODW}} = $markup; }
      if ($$hashref{'mode'} == 5) { ${$$hashref{$MODR}} = $markup; ${$$hashref{$MODX}} = $markup; }
      if ($$hashref{'mode'} == 3) { ${$$hashref{$MODW}} = $markup; ${$$hashref{$MODX}} = $markup; }
    }

    my $HTML = FileParser->new($HTML_CHMOD);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ changeModeFinal ────────────────────────────────────────────────────────────────────────────┐
#│ Return the Copy File page.                                                                  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub changeModeFinal {
  unless ($fileObject) {
    returnError("No file or directory was provided.  Can't change permissions on nothing!");
  }

  my $octmod = (stat $fileObject)[2];
  my $mod    = sprintf "%o", $octmod;
  my $preMod = substr($mod, 0, (length($mod) - 3));
  my $MODR   = 'read';
  my $MODW   = 'write';
  my $MODX   = 'execute';
  my %modo = (
      mode => 0,
     $MODR => \$modor,
     $MODW => \$modow,
     $MODX => \$modox
             );
  my %modg = (
     mode  => 0,
     $MODR => \$modgr,
     $MODW => \$modgw,
     $MODX => \$modgx
             );
  my %moda = (
     mode  => 0,
     $MODR => \$modar,
     $MODW => \$modaw,
     $MODX => \$modax
             );

  my @modeSlices = (\%modo, \%modg, \%moda);

  foreach my $hashref (@modeSlices) {
    SWITCH: {
      if (${$$hashref{$MODR}} && ${$$hashref{$MODW}} && ${$$hashref{$MODX}}) { $$hashref{'mode'} = 7; last SWITCH; }
      if (${$$hashref{$MODR}} && ${$$hashref{$MODW}})                        { $$hashref{'mode'} = 6; last SWITCH; }
      if (${$$hashref{$MODR}} && ${$$hashref{$MODX}})                        { $$hashref{'mode'} = 5; last SWITCH; }
      if (${$$hashref{$MODW}} && ${$$hashref{$MODX}})                        { $$hashref{'mode'} = 3; last SWITCH; }
      if (${$$hashref{$MODR}})                                               { $$hashref{'mode'} = 4; last SWITCH; }
      if (${$$hashref{$MODW}})                                               { $$hashref{'mode'} = 2; last SWITCH; }
      if (${$$hashref{$MODX}})                                               { $$hashref{'mode'} = 1; last SWITCH; }
    }
  }

  $octmod = oct $preMod.$modo{'mode'}.$modg{'mode'}.$moda{'mode'};

  if (not chmod $octmod, $fileObject) {
    returnError("Could not CHMOD on $fileObject");
  } else {
    getFileListing();
  }
}

# downloadFile
# 
# Give them the file, with a header forcing save-as dialog
#
# Chris Michaelis May 2008
sub downloadFile {
	print "Content-Type: application/x-download\n";  
	print "Content-Length: ".( -s "$current_path/$fileName")."\n";  
	print "Content-Disposition: attachment;filename=$fileName\n\n"; 
	if (open(DOWNLOAD, "<$current_path/$fileName"))
	{
		my $buffer;
		while (read(DOWNLOAD, $buffer, 32768))
		{
			print $buffer;
		}
		close(DOWNLOAD);
	}
}

#┌ copyFile ───────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Copy File page.                                                                  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub copyFile {
  $new_dir_name = $current_path unless $new_dir_name;
  $new_dir_name .= '/' unless index($new_dir_name, '/', (length($new_dir_name) -1)) > -1;

  if ($new_dir_name ne $DEFAULT_DIR) {
    $up_one_dir = chopLastDir($new_dir_name);
  } else {
    $up_one_dir = $new_dir_name;
  }

  getLSList($new_dir_name);
  my $HTML = FileParser->new($HTML_COPY_FILE);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ copyFileConfirm ────────────────────────────────────────────────────────────────────────────┐
#│ Return the Copy File Confirm page.                                                          │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub copyFileConfirm {
  $old_file_name = $current_path.$old_file_name;
  my $HTML = FileParser->new($HTML_COPY_FILE_C);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ copyFileFinal ──────────────────────────────────────────────────────────────────────────────┐
#│ Copy the file.  Return an error if unsuccessful or a success page if successful.            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub copyFileFinal {
  my $error;

  if($old_file_name eq $new_file_name) {
    $error = "Source file and destination are the same";
  } else {
    copy($old_file_name, $new_file_name) or $error = "Can't copy $old_file_name! $!";
  }

  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_COPY_FILE_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ moveFile ───────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Move File page.                                                                  │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub moveFile {
  $new_dir_name = $current_path unless $new_dir_name;
  $new_dir_name .= '/' unless index($new_dir_name, '/', (length($new_dir_name) -1)) > -1;

  if ($new_dir_name ne $DEFAULT_DIR) {
    $up_one_dir = chopLastDir($new_dir_name);
  } else {
    $up_one_dir = $new_dir_name;
  }

  getLSList($new_dir_name);
  my $HTML = FileParser->new($HTML_MOVE_FILE);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ moveFileConfirm ────────────────────────────────────────────────────────────────────────────┐
#│ Return the Move File Confirm page.                                                          │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub moveFileConfirm {
  $old_file_name = $current_path.$old_file_name;
  my $HTML = FileParser->new($HTML_MOVE_FILE_C);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ moveFileFinal ──────────────────────────────────────────────────────────────────────────────┐
#│ Move the file.  Return an error if unsuccessful or a success page if successful.            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub moveFileFinal {
  my $error;
  if($old_file_name eq $new_file_name) {
    $error = "Source file and destination are the same";
  } else {
  copy($old_file_name, $new_file_name) or $error = "Can't move $old_file_name! $!";
  unless ($error) {
    unlink($old_file_name) or $error .= " Can't remove $old_file_name! $!";
  }
  }
  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_MOVE_FILE_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ renameFile ─────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Rename File page.                                                                │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameFile {
  my $HTML = FileParser->new($HTML_RENAME_FILE);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ renameFileConfirm ──────────────────────────────────────────────────────────────────────────┐
#│ Return the Rename File Confirm page.                                                        │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameFileConfirm {
  my $old_dir = $old_file_name;
  $old_dir    =~ s /(.*\/).*/$1/;

  $new_file_name = $old_dir."$new_file_name";

  my $HTML = FileParser->new($HTML_RENAME_C);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ renameFileFinal ────────────────────────────────────────────────────────────────────────────┐
#│ Rename the file.  Return an error if unsuccessful or a success page if successful.          │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub renameFileFinal {
  my $error;

  rename ($old_file_name, $new_file_name) or $error = "Cannot rename file! $!";

  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_RENAME_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ deleteFile ─────────────────────────────────────────────────────────────────────────────────┐
#│ Return the Delete File page.                                                                │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub deleteFile {
  my $HTML = FileParser->new($HTML_DELETE_FL_C);
  $HTML->parseHTML(%HTMLParseVar);
  $HTML->printHTML();
}

#┌ deleteFileFinal ────────────────────────────────────────────────────────────────────────────┐
#│ Delete the file.  Return an error if unsuccessful or a success page if successful.          │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub deleteFileFinal {
  my $error;

  $error = "$old_file_name does not exists or is not a file" unless -f $old_file_name;

  unlink ($old_file_name) or $error = "Can't delete $old_file_name! $!";

  if ($error) {
    returnError($error);
  } else {
    my $HTML = FileParser->new($HTML_DELETE_FL_S);
    $HTML->parseHTML(%HTMLParseVar);
    $HTML->printHTML();
  }
}

#┌ getLSList ──────────────────────────────────────────────────────────────────────────────────┐
#│ Issue an 'ls' system command and parse the results.                                         │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getLSList {
  my $listPath = shift;
  my $DIR  = 'd';
  my $FILE = '-';
  my $LINK = 'l';
  $listPath =~ s/'/\\'/g;
  my $result = `ls -l '$listPath'`;
  my @dir_list = split (/\n/, $result);
  my $total = shift(@dir_list);

  # 0>Mode, 1>Type, 2>UID, 3>GID, 4>Size, 5>Month, 6>Day, 7>Year, 8>Filename
  foreach (@dir_list) {
    # my (@attribs, $filename) = (/^\s*(\S+) +(\S+) +(\S+) +(\S+) +(\S+) +(\S+) +(\S+) +(\S+) +(.*)/);
    my @attribs = split(' ', $_, 9);
    my $sfilename = $attribs[8];
    my $filename = $attribs[8];
	$sfilename =~ s/([^A-Za-z0-9])/sprintf("%%%02X", ord($1))/seg;
    my $fullPath = $listPath.$filename;

    $foType{$filename} = $attribs[1];
    $foMode{$filename} = $attribs[0];
    $foUID{$filename}  = $attribs[2];
    $foGID{$filename}  = $attribs[3];
    $foSize{$filename} = $attribs[4];
    $foLMDate{$filename} = "$attribs[5] $attribs[6] $attribs[7]";
    $escaped{$filename} = $sfilename;

    push (@dirNameKeys,  $filename) if -d $fullPath;
    push (@fileNameKeys, $filename) if -f $fullPath;
    push (@foKeys, $filename);
  }
}

#┌ getFileObjectAttributes ────────────────────────────────────────────────────────────────────┐
#│ Determine the identity and attributes of a file object.                                     │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getFileObjectAttributes {
  my $fileObject = shift;
  my ($foType, $foName, $foSize, $foMode, $foUID, $foGID, $foLMDate);
  my ($mode, $UID, $GID, $size, $mtime) = (stat($fileObject))[2,4,5,7,11];

  SWITCH: {
    if (-f $fileObject) { $foType = $FILE;      last SWITCH; }
    if (-d $fileObject) { $foType = $DIRECTORY; last SWITCH; }
    $fileObject = $UNKNOWN;
  }

  $foType == $FILE ? $foSize = $size : $foSize = 0;
  $foName   = $fileObject;
  $foName   =~ s#.*/##;
  $foMode   = $mode;
  $foUID    = $UID;
  $foGID    = $GID;
  $foSize   = $size;
  $foLMDate = $mtime;

  return ($foType, $foName, $foMode, $foUID, $foGID, $foSize, $foLMDate);
} # end getFileObjectAttributes

#┌ chopLastDir ────────────────────────────────────────────────────────────────────────────────┐
#│ Return the path sans the last directory in the path.                                        │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub chopLastDir {
  my $dir = shift;

  chop($dir);
  my $lastSlash = rindex($dir, '/');
  $dir = substr($dir, 0, $lastSlash);
  $dir = '/' if $dir eq '';
  return $dir;
}

#┌ getEditable ────────────────────────────────────────────────────────────────────────────────┐
#│ Determine whether a file object is editable.                                                │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getEditable {
  my $fo       = shift;
  my $editable = 0;
  my $error;

  SWITCH: {
      if (not -e $fo) { $error = "The file you are trying to edit does not exist! $!"; last SWITCH; }
      if (not -f $fo) { $error = "Whatever you are attempting to edit is not a file! $!"; last SWITCH; }
      if (not -T $fo) { $error = "That type of file cannot be modified from this file manager."; last SWITCH; }
      $editable = 1;
  }
  return ($editable, $error);
}

#┌ getViewable ────────────────────────────────────────────────────────────────────────────────┐
#│ Determine whether a file object is viewable.                                                │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getViewable {
  my $fo       = shift;
  my $viewable = 0;
  my $error;
  my $fileType;

  SWITCH: {
      if (not -e $fo) { $error = "The file you are trying to view does not exist! $!"; last SWITCH; }
      if (not -f $fo) { $error = "Whatever you are attempting to view is not a file! $!"; last SWITCH; }
      if (-T $fo)     { $fileType = "txt"; $viewable = 1; last SWITCH; }
      if (-B $fo)     { $fileType = "bin"; last SWITCH; }
  }

  if ($fileType eq "bin") {
    $fo =~ s#(?:.*\.)##;
    SWITCH: {
        if ($fo eq "gif")  { $fileType = "gif";  $viewable = 1; last SWITCH; }
        if ($fo eq "jpg")  { $fileType = "jpeg"; $viewable = 1; last SWITCH; }
        if ($fo eq "jpeg") { $fileType = "jpeg"; $viewable = 1; last SWITCH; }
        if ($fo eq "png")  { $fileType = "png";  $viewable = 1; last SWITCH; }
        if ($fo eq "swf")  { $fileType = "swf";  $viewable = 1; last SWITCH; }
    }
  }

  return ($viewable, $fileType, $error);
}

#┌ populateCurDir ─────────────────────────────────────────────────────────────────────────────┐
#│ Populate %curDirPath, %curDirName and @curDirKeys with current path information.            │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub populateCurDir {
  my $myPath = '/';
  @curDirKeys = ($myPath);
  $curDirPath{$myPath} = $myPath;
  $curDirName{$myPath} = "$myPath ";

  if ($current_path eq '/') {
    return;
  } else {
    my @dirSplits = split(/\//, $current_path);
    shift(@dirSplits);

    for (my $i = 0; $i < @dirSplits; $i++) {
      push (@curDirKeys, $i.$dirSplits[$i]);
      $myPath .= $dirSplits[$i]."/";
      $curDirPath{$i.$dirSplits[$i]} = $myPath;
      $curDirName{$i.$dirSplits[$i]} = $dirSplits[$i].' / ';
    }
  }
}

#┌ dbLog ──────────────────────────────────────────────────────────────────────────────────────┐
#│ Save to the log file.                                                                       │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub dbLog {
  my $line = shift;

  unless (-f $debug_log) {
    open (LOG, ">$debug_log");
    print LOG "$line\n";
    close (LOG);
    return;
  }

  open (LOG, ">>$debug_log");
  print LOG "$line\n";
  close (LOG);
}

#┌ checkCriticalVars ──────────────────────────────────────────────────────────────────────────┐
#│ Ensure that all "critical variables" are present before allowing use of this app.           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub checkCriticalVars {
  my @critial_vars = @_;

  foreach (@critial_vars) {
    die "Missing a critical variable!  Can't continue!" unless ${$_};
  }
}

#┌ getConfig ──────────────────────────────────────────────────────────────────────────────────┐
#│ Read in the configuration for this application from the configuration file.                 │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getConfig {
  my $config_file = shift;
  my %config;

  open (CONFIG, $config_file);
  foreach (<CONFIG>) {
    next if /^#/;

    if (/=/) {
      my ($key, $value) = split(/=/);
      chomp($value);
      $config{$key} = $value;
    }
  }

  return %config;
}

#┌ escapeHTML ─────────────────────────────────────────────────────────────────────────────────┐
#│ Escape any "dangerous" characters so that the browser doesn't improperly translate the page.│
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub escapeHTML {
  my $text = shift;
  my %map = getEscapeMap();

  $text =~ s/\&/\&#38;/gi;

  foreach my $key (keys %map) {
    $text =~ s/$key/$map{$key}/gi;
  }

  return $text;
}

#┌ unescapeHTML ───────────────────────────────────────────────────────────────────────────────┐
#│ Unescape any escaping.                                                                      │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub unescapeHTML {
  my $text = shift;
  my %map = getEscapeMap();

  $text =~ s/\&#38;/\&/gi;

  foreach my $key (keys %map) {
    $text =~ s/$map{$key}/$key/gi;
  }

  return $text;
}

#┌ getEscapeMap ───────────────────────────────────────────────────────────────────────────────┐
#│ Explain what needs to be escaped.                                                           │
#└─────────────────────────────────────────────────────────────────────────────────────────────┘
sub getEscapeMap {
  my %map = (
      '<textarea'   => '&lt;textarea',
      '</textarea>' => '&lt;/textarea&gt;'
            );
  return %map;
}
