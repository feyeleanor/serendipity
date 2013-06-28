/*
** This file contains code to implement the "sqlite" command line
** utility for accessing SQLite databases.
*/

/*
** Enable large-file support for fopen() and friends on unix.
*/
# define _LARGE_FILE       1
# ifndef _FILE_OFFSET_BITS
#   define _FILE_OFFSET_BITS 64
# endif
# define _LARGEFILE_SOURCE 1

#if !defined(HAVE_EDITLINE) && (!defined(HAVE_READLINE) || HAVE_READLINE!=1)
# define readline(p) local_getline(p,stdin,0)
# define add_history(X)
# define read_history(X)
# define write_history(X)
# define stifle_history(X)
#endif


/* True if the timer is enabled */
int enableTimer = 0;

/* ctype macros that work with signed characters */
#define IsSpace(X)  isspace((unsigned char)X)
#define IsDigit(X)  isdigit((unsigned char)X)

#if !defined(_WRS_KERNEL) && !defined(__minux)

/* Saved resource information for the beginning of an operation */
struct rusage sBegin;

/*
** Begin timing an operation
*/
void beginTimer(void){
  if( enableTimer ){
    getrusage(RUSAGE_SELF, &sBegin);
  }
}

/* Return the difference of two time_structs in seconds */
float64 timeDiff(struct timeval *pStart, struct timeval *pEnd){
  return (pEnd->tv_usec - pStart->tv_usec)*0.000001 + 
         (float64)(pEnd->tv_sec - pStart->tv_sec);
}

/*
** Print the timing results.
*/
void endTimer(void){
  if( enableTimer ){
    struct rusage sEnd;
    getrusage(RUSAGE_SELF, &sEnd);
    printf("CPU Time: user %f sys %f\n",
       timeDiff(&sBegin.ru_utime, &sEnd.ru_utime),
       timeDiff(&sBegin.ru_stime, &sEnd.ru_stime));
  }
}

#define BEGIN_TIMER beginTimer()
#define END_TIMER endTimer()
#define HAS_TIMER 1

#else
#define BEGIN_TIMER 
#define END_TIMER
#define HAS_TIMER 0
#endif

/*
** Used to prevent warnings about unused parameters
*/
#define UNUSED_PARAMETER(x) (void)(x)

/*
** If the following flag is set, then command execution stops
** at an error if we are not interactive.
*/
int bail_on_error = 0;

/*
** Threat stdin as an interactive input if the following variable
** is true.  Otherwise, assume stdin is connected to a file or pipe.
*/
int stdin_is_interactive = 1;

/*
** The following is the open SQLite database.  We make a pointer
** to this database a variable so that it can be accessed
** by the SIGINT handler to interrupt database processing.
*/
sqlite3 *db = 0;

/*
** True if an interrupt (Control-C) has been received.
*/
volatile int seenInterrupt = 0;

/*
** This is the name of our program. It is set in main(), used
** in a number of other places, mostly for error messages.
*/
char *Argv0;

/*
** Prompt strings. Initialized in main. Settable with
**   .prompt main continue
*/
char mainPrompt[20];     /* First line prompt. default: "sqlite> "*/
char continuePrompt[20]; /* Continuation prompt. default: "   ...> " */

/*
** Determines if a string is a number of not.
*/
int isNumber(const char *z, int *realnum){
  if( *z=='-' || *z=='+' ) z++;
  if( !IsDigit(*z) ){
    return 0;
  }
  z++;
  if( realnum ) *realnum = 0;
  while( IsDigit(*z) ){ z++; }
  if( *z=='.' ){
    z++;
    if( !IsDigit(*z) ) return 0;
    while( IsDigit(*z) ){ z++; }
    if( realnum ) *realnum = 1;
  }
  if( *z=='e' || *z=='E' ){
    z++;
    if( *z=='+' || *z=='-' ) z++;
    if( !IsDigit(*z) ) return 0;
    while( IsDigit(*z) ){ z++; }
    if( realnum ) *realnum = 1;
  }
  return *z==0;
}

/*
** A global char* and an SQL function to access its current value 
** from within an SQL statement. This program used to use the 
** sqlite_exec_printf() API to substitue a string into an SQL statement.
** The correct way to do this with sqlite3 is to use the bind API, but
** since the shell is built around the callback paradigm it would be a lot
** of work. Instead just use this hack, which is quite harmless.
*/
const char *zShellStatic = 0;
void shellstaticFunc(
  sqlite3_context *context,
  int argc,
  sqlite3_value **argv
){
  assert( 0==argc );
  assert( zShellStatic );
  UNUSED_PARAMETER(argc);
  UNUSED_PARAMETER(argv);
  sqlite3_result_text(context, zShellStatic, -1, SQLITE_STATIC);
}


/*
** This routine reads a line of text from FILE in, stores
** the text in memory obtained from malloc() and returns a pointer
** to the text.  NULL is returned at end of file, or if malloc()
** fails.
**
** The interface is like "readline" but no command-line editing
** is done.
*/
char *local_getline(char *zPrompt, FILE *in, int csvFlag){
  char *zLine;
  int nLine;
  int n;
  int inQuote = 0;

  if( zPrompt && *zPrompt ){
    printf("%s",zPrompt);
    fflush(stdout);
  }
  nLine = 100;
  zLine = malloc( nLine );
  if( zLine==0 ) return 0;
  n = 0;
  while( 1 ){
    if( n+100>nLine ){
      nLine = nLine*2 + 100;
      zLine = realloc(zLine, nLine);
      if( zLine==0 ) return 0;
    }
    if( fgets(&zLine[n], nLine - n, in)==0 ){
      if( n==0 ){
        free(zLine);
        return 0;
      }
      zLine[n] = 0;
      break;
    }
    while( zLine[n] ){
      if( zLine[n]=='"' ) inQuote = !inQuote;
      n++;
    }
    if( n>0 && zLine[n-1]=='\n' && (!inQuote || !csvFlag) ){
      n--;
      if( n>0 && zLine[n-1]=='\r' ) n--;
      zLine[n] = 0;
      break;
    }
  }
  zLine = realloc( zLine, n+1 );
  return zLine;
}

/*
** Retrieve a single line of input text.
**
** zPrior is a string of prior text retrieved.  If not the empty
** string, then issue a continuation prompt.
*/
char *one_input_line(const char *zPrior, FILE *in){
  char *zPrompt;
  char *zResult;
  if( in!=0 ){
    return local_getline(0, in, 0);
  }
  if( zPrior && zPrior[0] ){
    zPrompt = continuePrompt;
  }else{
    zPrompt = mainPrompt;
  }
  zResult = readline(zPrompt);
#if defined(HAVE_READLINE) && HAVE_READLINE==1
  if( zResult && *zResult ) add_history(zResult);
#endif
  return zResult;
}

struct previous_mode_data {
  int valid;        /* Is there legit data in here? */
  int mode;
  int showHeader;
  int colWidth[100];
};

/*
** An pointer to an instance of this structure is passed from
** the main program to the callback.  This is used to communicate
** state and mode information.
*/
struct callback_data {
  sqlite3 *db;           /* The database */
  int echoOn;            /* True to echo input commands */
  int statsOn;           /* True to display memory stats before each finalize */
  int cnt;               /* Number of records displayed so far */
  FILE *out;             /* Write results here */
  FILE *traceOut;        /* Output for sqlite3_trace() */
  int nErr;              /* Number of errors seen */
  int mode;              /* An output mode setting */
  int writableSchema;    /* True if PRAGMA writable_schema=ON */
  int showHeader;        /* True to show column names in List or Column mode */
  char *zDestTable;      /* Name of destination table when MODE_Insert */
  char separator[20];    /* Separator character for MODE_List */
  int colWidth[100];     /* Requested width of each column when in column mode*/
  int actualWidth[100];  /* Actual width of each column */
  char nullvalue[20];    /* The text to print when a NULL comes back from
                         ** the database */
  struct previous_mode_data explainPrev;
                         /* Holds the mode information just before
                         ** .explain ON */
  char outfile[FILENAME_MAX]; /* Filename for *out */
  const char *zDbFilename;    /* name of the database file */
  const char *zVfs;           /* Name of VFS to use */
  sqlite3_stmt *pStmt;   /* Current statement if any. */
  FILE *pLog;            /* Write log output here */
};

/*
** These are the allowed modes.
*/
#define MODE_Line     0  /* One column per line.  Blank line between records */
#define MODE_Column   1  /* One record per line in neat columns */
#define MODE_List     2  /* One record per line with a separator */
#define MODE_Semi     3  /* Same as MODE_List but append ";" to each line */
#define MODE_Html     4  /* Generate an XHTML table */
#define MODE_Insert   5  /* Generate SQL "insert" statements */
#define MODE_Tcl      6  /* Generate ANSI-C or TCL quoted elements */
#define MODE_Csv      7  /* Quote strings, numbers are plain */
#define MODE_Explain  8  /* Like MODE_Column, but do not truncate data */

const char *modeDescr[] = {
  "line",
  "column",
  "list",
  "semi",
  "html",
  "insert",
  "tcl",
  "csv",
  "explain",
};

/*
** Number of elements in an array
*/
#define ArraySize(X)  (int)(sizeof(X)/sizeof(X[0]))

/*
** Compute a string length that is limited to what can be stored in
** lower 30 bits of a 32-bit signed integer.
*/
int strlen30(const char *z){
  const char *z2 = z;
  while( *z2 ){ z2++; }
  return 0x3fffffff & (int)(z2 - z);
}

/*
** A callback for the sqlite3_log() interface.
*/
void shellLog(void *pArg, int iErrCode, const char *zMsg){
  struct callback_data *p = (struct callback_data*)pArg;
  if( p->pLog==0 ) return;
  fprintf(p->pLog, "(%d) %s\n", iErrCode, zMsg);
  fflush(p->pLog);
}

/*
** Output the given string as a hex-encoded blob (eg. X'1234' )
*/
void output_hex_blob(FILE *out, const void *pBlob, int nBlob){
  int i;
  char *zBlob = (char *)pBlob;
  fprintf(out,"X'");
  for(i=0; i<nBlob; i++){ fprintf(out,"%02x",zBlob[i]&0xff); }
  fprintf(out,"'");
}

/*
** Output the given string as a quoted string using SQL quoting conventions.
*/
void output_quoted_string(FILE *out, const char *z){
  int i;
  int nSingle = 0;
  for(i=0; z[i]; i++){
    if( z[i]=='\'' ) nSingle++;
  }
  if( nSingle==0 ){
    fprintf(out,"'%s'",z);
  }else{
    fprintf(out,"'");
    while( *z ){
      for(i=0; z[i] && z[i]!='\''; i++){}
      if( i==0 ){
        fprintf(out,"''");
        z++;
      }else if( z[i]=='\'' ){
        fprintf(out,"%.*s''",i,z);
        z += i+1;
      }else{
        fprintf(out,"%s",z);
        break;
      }
    }
    fprintf(out,"'");
  }
}

/*
** Output the given string as a quoted according to C or TCL quoting rules.
*/
void output_c_string(FILE *out, const char *z){
  unsigned int c;
  fputc('"', out);
  while( (c = *(z++))!=0 ){
    if( c=='\\' ){
      fputc(c, out);
      fputc(c, out);
    }else if( c=='"' ){
      fputc('\\', out);
      fputc('"', out);
    }else if( c=='\t' ){
      fputc('\\', out);
      fputc('t', out);
    }else if( c=='\n' ){
      fputc('\\', out);
      fputc('n', out);
    }else if( c=='\r' ){
      fputc('\\', out);
      fputc('r', out);
    }else if( !isprint(c) ){
      fprintf(out, "\\%03o", c&0xff);
    }else{
      fputc(c, out);
    }
  }
  fputc('"', out);
}

/*
** Output the given string with characters that are special to
** HTML escaped.
*/
void output_html_string(FILE *out, const char *z){
  int i;
  while( *z ){
    for(i=0;   z[i] 
            && z[i]!='<' 
            && z[i]!='&' 
            && z[i]!='>' 
            && z[i]!='\"' 
            && z[i]!='\'';
        i++){}
    if( i>0 ){
      fprintf(out,"%.*s",i,z);
    }
    if( z[i]=='<' ){
      fprintf(out,"&lt;");
    }else if( z[i]=='&' ){
      fprintf(out,"&amp;");
    }else if( z[i]=='>' ){
      fprintf(out,"&gt;");
    }else if( z[i]=='\"' ){
      fprintf(out,"&quot;");
    }else if( z[i]=='\'' ){
      fprintf(out,"&#39;");
    }else{
      break;
    }
    z += i + 1;
  }
}

/*
** If a field contains any character identified by a 1 in the following
** array, then the string must be quoted for CSV.
*/
const char needCsvQuote[] = {
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 0, 1, 0, 0, 0, 0, 1,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 0, 
  0, 0, 0, 0, 0, 0, 0, 0,   0, 0, 0, 0, 0, 0, 0, 1, 
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
  1, 1, 1, 1, 1, 1, 1, 1,   1, 1, 1, 1, 1, 1, 1, 1,   
};

/*
** Output a single term of CSV.  Actually, p->separator is used for
** the separator, which may or may not be a comma.  p->nullvalue is
** the null value.  Strings are quoted if necessary.
*/
void output_csv(struct callback_data *p, const char *z, int bSep){
  FILE *out = p->out;
  if( z==0 ){
    fprintf(out,"%s",p->nullvalue);
  }else{
    int i;
    int nSep = strlen30(p->separator);
    for(i=0; z[i]; i++){
      if( needCsvQuote[((unsigned char*)z)[i]] 
         || (z[i]==p->separator[0] && 
             (nSep==1 || memcmp(z, p->separator, nSep)==0)) ){
        i = 0;
        break;
      }
    }
    if( i==0 ){
      putc('"', out);
      for(i=0; z[i]; i++){
        if( z[i]=='"' ) putc('"', out);
        putc(z[i], out);
      }
      putc('"', out);
    }else{
      fprintf(out, "%s", z);
    }
  }
  if( bSep ){
    fprintf(p->out, "%s", p->separator);
  }
}

#ifdef SIGINT
/*
** This routine runs when the user presses Ctrl-C
*/
void interrupt_handler(int NotUsed){
  UNUSED_PARAMETER(NotUsed);
  seenInterrupt = 1;
  if( db ) sqlite3_interrupt(db);
}
#endif

/*
** This is the callback routine that the shell
** invokes for each row of a query result.
*/
int shell_callback(void *pArg, int nArg, char **azArg, char **azCol, int *aiType){
  int i;
  struct callback_data *p = (struct callback_data*)pArg;

  switch( p->mode ){
    case MODE_Line: {
      int w = 5;
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int len = strlen30(azCol[i] ? azCol[i] : "");
        if( len>w ) w = len;
      }
      if( p->cnt++>0 ) fprintf(p->out,"\n");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"%*s = %s\n", w, azCol[i],
                azArg[i] ? azArg[i] : p->nullvalue);
      }
      break;
    }
    case MODE_Explain:
    case MODE_Column: {
      if( p->cnt++==0 ){
        for(i=0; i<nArg; i++){
          int w, n;
          if( i<ArraySize(p->colWidth) ){
            w = p->colWidth[i];
          }else{
            w = 0;
          }
          if( w==0 ){
            w = strlen30(azCol[i] ? azCol[i] : "");
            if( w<10 ) w = 10;
            n = strlen30(azArg && azArg[i] ? azArg[i] : p->nullvalue);
            if( w<n ) w = n;
          }
          if( i<ArraySize(p->actualWidth) ){
            p->actualWidth[i] = w;
          }
          if( p->showHeader ){
            if( w<0 ){
              fprintf(p->out,"%*.*s%s",-w,-w,azCol[i], i==nArg-1 ? "\n": "  ");
            }else{
              fprintf(p->out,"%-*.*s%s",w,w,azCol[i], i==nArg-1 ? "\n": "  ");
            }
          }
        }
        if( p->showHeader ){
          for(i=0; i<nArg; i++){
            int w;
            if( i<ArraySize(p->actualWidth) ){
               w = p->actualWidth[i];
               if( w<0 ) w = -w;
            }else{
               w = 10;
            }
            fprintf(p->out,"%-*.*s%s",w,w,"-----------------------------------"
                   "----------------------------------------------------------",
                    i==nArg-1 ? "\n": "  ");
          }
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        int w;
        if( i<ArraySize(p->actualWidth) ){
           w = p->actualWidth[i];
        }else{
           w = 10;
        }
        if( p->mode==MODE_Explain && azArg[i] && 
           strlen30(azArg[i])>w ){
          w = strlen30(azArg[i]);
        }
        if( w<0 ){
          fprintf(p->out,"%*.*s%s",-w,-w,
              azArg[i] ? azArg[i] : p->nullvalue, i==nArg-1 ? "\n": "  ");
        }else{
          fprintf(p->out,"%-*.*s%s",w,w,
              azArg[i] ? azArg[i] : p->nullvalue, i==nArg-1 ? "\n": "  ");
        }
      }
      break;
    }
    case MODE_Semi:
    case MODE_List: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          fprintf(p->out,"%s%s",azCol[i], i==nArg-1 ? "\n" : p->separator);
        }
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        char *z = azArg[i];
        if( z==0 ) z = p->nullvalue;
        fprintf(p->out, "%s", z);
        if( i<nArg-1 ){
          fprintf(p->out, "%s", p->separator);
        }else if( p->mode==MODE_Semi ){
          fprintf(p->out, ";\n");
        }else{
          fprintf(p->out, "\n");
        }
      }
      break;
    }
    case MODE_Html: {
      if( p->cnt++==0 && p->showHeader ){
        fprintf(p->out,"<TR>");
        for(i=0; i<nArg; i++){
          fprintf(p->out,"<TH>");
          output_html_string(p->out, azCol[i]);
          fprintf(p->out,"</TH>\n");
        }
        fprintf(p->out,"</TR>\n");
      }
      if( azArg==0 ) break;
      fprintf(p->out,"<TR>");
      for(i=0; i<nArg; i++){
        fprintf(p->out,"<TD>");
        output_html_string(p->out, azArg[i] ? azArg[i] : p->nullvalue);
        fprintf(p->out,"</TD>\n");
      }
      fprintf(p->out,"</TR>\n");
      break;
    }
    case MODE_Tcl: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          output_c_string(p->out,azCol[i] ? azCol[i] : "");
          if(i<nArg-1) fprintf(p->out, "%s", p->separator);
        }
        fprintf(p->out,"\n");
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        output_c_string(p->out, azArg[i] ? azArg[i] : p->nullvalue);
        if(i<nArg-1) fprintf(p->out, "%s", p->separator);
      }
      fprintf(p->out,"\n");
      break;
    }
    case MODE_Csv: {
      if( p->cnt++==0 && p->showHeader ){
        for(i=0; i<nArg; i++){
          output_csv(p, azCol[i] ? azCol[i] : "", i<nArg-1);
        }
        fprintf(p->out,"\n");
      }
      if( azArg==0 ) break;
      for(i=0; i<nArg; i++){
        output_csv(p, azArg[i], i<nArg-1);
      }
      fprintf(p->out,"\n");
      break;
    }
    case MODE_Insert: {
      p->cnt++;
      if( azArg==0 ) break;
      fprintf(p->out,"INSERT INTO %s VALUES(",p->zDestTable);
      for(i=0; i<nArg; i++){
        char *zSep = i>0 ? ",": "";
        if( (azArg[i]==0) || (aiType && aiType[i]==SQLITE_NULL) ){
          fprintf(p->out,"%sNULL",zSep);
        }else if( aiType && aiType[i]==SQLITE_TEXT ){
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_quoted_string(p->out, azArg[i]);
        }else if( aiType && (aiType[i]==SQLITE_INTEGER || aiType[i]==SQLITE_FLOAT) ){
          fprintf(p->out,"%s%s",zSep, azArg[i]);
        }else if( aiType && aiType[i]==SQLITE_BLOB && p->pStmt ){
          const void *pBlob = ppStmt.ColumnBlob(i)
          int nBlob = p.pStmt.ColumnBytes(i)
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_hex_blob(p->out, pBlob, nBlob);
        }else if( isNumber(azArg[i], 0) ){
          fprintf(p->out,"%s%s",zSep, azArg[i]);
        }else{
          if( zSep[0] ) fprintf(p->out,"%s",zSep);
          output_quoted_string(p->out, azArg[i]);
        }
      }
      fprintf(p->out,");\n");
      break;
    }
  }
  return 0;
}

/*
** This is the callback routine that the SQLite library
** invokes for each row of a query result.
*/
int callback(void *pArg, int nArg, char **azArg, char **azCol){
  /* since we don't have type info, call the shell_callback with a NULL value */
  return shell_callback(pArg, nArg, azArg, azCol, NULL);
}

/*
** Set the destination table field of the callback_data structure to
** the name of the table given.  Escape any quote characters in the
** table name.
*/
void set_table_name(struct callback_data *p, const char *zName){
  int i, n;
  int needQuote;
  char *z;

  if( p->zDestTable ){
    free(p->zDestTable);
    p->zDestTable = 0;
  }
  if( zName==0 ) return;
  needQuote = !isalpha((unsigned char)*zName) && *zName!='_';
  for(i=n=0; zName[i]; i++, n++){
    if( !isalnum((unsigned char)zName[i]) && zName[i]!='_' ){
      needQuote = 1;
      if( zName[i]=='\'' ) n++;
    }
  }
  if( needQuote ) n += 2;
  z = p->zDestTable = malloc( n+1 );
  if( z==0 ){
    fprintf(stderr,"Error: out of memory\n");
    exit(1);
  }
  n = 0;
  if( needQuote ) z[n++] = '\'';
  for(i=0; zName[i]; i++){
    z[n++] = zName[i];
    if( zName[i]=='\'' ) z[n++] = '\'';
  }
  if( needQuote ) z[n++] = '\'';
  z[n] = 0;
}

/* zIn is either a pointer to a NULL-terminated string in memory obtained
** from malloc(), or a NULL pointer. The string pointed to by zAppend is
** added to zIn, and the result returned in memory obtained from malloc().
** zIn, if it was not NULL, is freed.
**
** If the third argument, quote, is not '\0', then it is used as a 
** quote character for zAppend.
*/
char *appendText(char *zIn, char const *zAppend, char quote){
  int len;
  int i;
  int nAppend = strlen30(zAppend);
  int nIn = (zIn?strlen30(zIn):0);

  len = nAppend+nIn+1;
  if( quote ){
    len += 2;
    for(i=0; i<nAppend; i++){
      if( zAppend[i]==quote ) len++;
    }
  }

  zIn = (char *)realloc(zIn, len);
  if( !zIn ){
    return 0;
  }

  if( quote ){
    char *zCsr = &zIn[nIn];
    *zCsr++ = quote;
    for(i=0; i<nAppend; i++){
      *zCsr++ = zAppend[i];
      if( zAppend[i]==quote ) *zCsr++ = quote;
    }
    *zCsr++ = quote;
    *zCsr++ = '\0';
    assert( (zCsr-zIn)==len );
  }else{
    memcpy(&zIn[nIn], zAppend, nAppend);
    zIn[len-1] = '\0';
  }

  return zIn;
}


/*
** Execute a query statement that will generate SQL output.  Print
** the result columns, comma-separated, on a line and then add a
** semicolon terminator to the end of that line.
**
** If the number of columns is 1 and that column contains text "--"
** then write the semicolon on a separate line.  That way, if a 
** "--" comment occurs at the end of the statement, the comment
** won't consume the semicolon terminator.
*/
int run_table_dump_query(
  struct callback_data *p, /* Query context */
  const char *zSelect,     /* SELECT statement to extract content */
  const char *zFirstRow    /* Print before first row, if not NULL */
){
  sqlite3_stmt *pSelect;
  int rc;
  int nResult;
  int i;
  const char *z;
  rc = sqlite3_prepare(p->db, zSelect, -1, &pSelect, 0);
  if( rc!=SQLITE_OK || !pSelect ){
    fprintf(p->out, "/**** ERROR: (%d) %s *****/\n", rc, sqlite3_errmsg(p->db));
    p->nErr++;
    return rc;
  }
  rc = pSelect.Step()
  nResult = sqlite3_column_count(pSelect);
  while( rc==SQLITE_ROW ){
    if( zFirstRow ){
      fprintf(p->out, "%s", zFirstRow);
      zFirstRow = 0;
    }
    z = (const char*)sqlite3_column_text(pSelect, 0);
    fprintf(p->out, "%s", z);
    for(i=1; i<nResult; i++){ 
      fprintf(p->out, ",%s", sqlite3_column_text(pSelect, i));
    }
    if( z==0 ) z = "";
    while( z[0] && (z[0]!='-' || z[1]!='-') ) z++;
    if( z[0] ){
      fprintf(p->out, "\n;\n");
    }else{
      fprintf(p->out, ";\n");
    }    
    rc = pSelect.Step()
  }
  rc = pSelect.Finalize()
  if( rc!=SQLITE_OK ){
    fprintf(p->out, "/**** ERROR: (%d) %s *****/\n", rc, sqlite3_errmsg(p->db));
    p->nErr++;
  }
  return rc;
}

//	Allocate space and save off current error string.
func save_err_msg(db *sqlite3) string {
	return CopyString(sqlite3_errmsg(db))
}

/*
** Display memory stats.
*/
int display_stats(
  sqlite3 *db,                /* Database to query */
  struct callback_data *pArg, /* Pointer to struct callback_data */
  int bReset                  /* True to reset the stats */
){
  int iCur;
  int iHiwtr;

  if( pArg && pArg->out ){
    
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_MEMORY_USED, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Memory Used:                         %d (max %d) bytes\n", iCur, iHiwtr);
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_MALLOC_COUNT, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Number of Outstanding Allocations:   %d (max %d)\n", iCur, iHiwtr);
/*
** Not currently used by the CLI.
**    iHiwtr = iCur = -1;
**    sqlite3_status(SQLITE_STATUS_PAGECACHE_USED, &iCur, &iHiwtr, bReset);
**    fprintf(pArg->out, "Number of Pcache Pages Used:         %d (max %d) pages\n", iCur, iHiwtr);
*/
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_PAGECACHE_OVERFLOW, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Number of Pcache Overflow Bytes:     %d (max %d) bytes\n", iCur, iHiwtr);
/*
** Not currently used by the CLI.
**    iHiwtr = iCur = -1;
**    sqlite3_status(SQLITE_STATUS_SCRATCH_USED, &iCur, &iHiwtr, bReset);
**    fprintf(pArg->out, "Number of Scratch Allocations Used:  %d (max %d)\n", iCur, iHiwtr);
*/
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_SCRATCH_OVERFLOW, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Number of Scratch Overflow Bytes:    %d (max %d) bytes\n", iCur, iHiwtr);
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_MALLOC_SIZE, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Largest Allocation:                  %d bytes\n", iHiwtr);
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_PAGECACHE_SIZE, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Largest Pcache Allocation:           %d bytes\n", iHiwtr);
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_SCRATCH_SIZE, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Largest Scratch Allocation:          %d bytes\n", iHiwtr);
#ifdef YYTRACKMAXSTACKDEPTH
    iHiwtr = iCur = -1;
    sqlite3_status(SQLITE_STATUS_PARSER_STACK, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Deepest Parser Stack:                %d (max %d)\n", iCur, iHiwtr);
#endif
  }

  if( pArg && pArg->out && db ){
    iHiwtr = iCur = -1;
    sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_USED, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Pager Heap Usage:                    %d bytes\n", iCur);    iHiwtr = iCur = -1;
    sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_HIT, &iCur, &iHiwtr, 1);
    fprintf(pArg->out, "Page cache hits:                     %d\n", iCur);
    iHiwtr = iCur = -1;
    sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_MISS, &iCur, &iHiwtr, 1);
    fprintf(pArg->out, "Page cache misses:                   %d\n", iCur); 
    iHiwtr = iCur = -1;
    sqlite3_db_status(db, SQLITE_DBSTATUS_CACHE_WRITE, &iCur, &iHiwtr, 1);
    fprintf(pArg->out, "Page cache writes:                   %d\n", iCur); 
    iHiwtr = iCur = -1;
    sqlite3_db_status(db, SQLITE_DBSTATUS_SCHEMA_USED, &iCur, &iHiwtr, bReset);
    fprintf(pArg->out, "Schema Heap Usage:                   %d bytes\n", iCur); 
  }

  if( pArg && pArg->out && db && pArg->pStmt ){
    iCur = sqlite3_stmt_status(pArg->pStmt, SQLITE_STMTSTATUS_FULLSCAN_STEP, bReset);
    fprintf(pArg->out, "Fullscan Steps:                      %d\n", iCur);
    iCur = sqlite3_stmt_status(pArg->pStmt, SQLITE_STMTSTATUS_SORT, bReset);
    fprintf(pArg->out, "Sort Operations:                     %d\n", iCur);
    iCur = sqlite3_stmt_status(pArg->pStmt, SQLITE_STMTSTATUS_AUTOINDEX, bReset);
    fprintf(pArg->out, "Autoindex Inserts:                   %d\n", iCur);
  }

  return 0;
}

/*
** Execute a statement or set of statements.  Print 
** any result rows/columns depending on the current mode 
** set via the supplied callback.
**
** This is very similar to SQLite's built-in sqlite3_exec() 
** function except it takes a slightly different callback 
** and callback data argument.
*/
int shell_exec(
  sqlite3 *db,                                /* An open database */
  const char *zSql,                           /* SQL to be evaluated */
  int (*xCallback)(void*,int,char**,char**,int*),   /* Callback function */
                                              /* (not the same as sqlite3_exec) */
  struct callback_data *pArg,                 /* Pointer to struct callback_data */
  char **pzErrMsg                             /* Error msg written here */
){
  sqlite3_stmt *pStmt = NULL;     /* Statement to execute. */
  int rc = SQLITE_OK;             /* Return Code */
  int rc2;
  const char *zLeftover;          /* Tail of unprocessed SQL */

  if( pzErrMsg ){
    *pzErrMsg = NULL;
  }

  while( zSql[0] && (SQLITE_OK == rc) ){
    pStmt, zLeftover, rc = db.Prepare_v2(zSql)
    if( SQLITE_OK != rc ){
      if( pzErrMsg ){
        *pzErrMsg = save_err_msg(db);
      }
    }else{
      if( !pStmt ){
        /* this happens for a comment or white-space */
        zSql = zLeftover;
        while( IsSpace(zSql[0]) ) zSql++;
        continue;
      }

      /* save off the prepared statment handle and reset row count */
      if( pArg ){
        pArg->pStmt = pStmt;
        pArg->cnt = 0;
      }

      /* echo the sql statement if echo on */
      if( pArg && pArg->echoOn ){
        const char *zStmtSql = sqlite3_sql(pStmt);
        fprintf(pArg->out, "%s\n", zStmtSql ? zStmtSql : zSql);
      }

      /* Output TESTCTRL_EXPLAIN text of requested */
      if( pArg && pArg->mode==MODE_Explain ){
        const char *zExplain = 0;
        sqlite3_test_control(SQLITE_TESTCTRL_EXPLAIN_STMT, pStmt, &zExplain);
        if( zExplain && zExplain[0] ){
          fprintf(pArg->out, "%s", zExplain);
        }
      }

      /* perform the first step.  this will tell us if we
      ** have a result set or not and how wide it is.
      */
      rc = pStmt.Step()
      /* if we have a result set... */
      if( SQLITE_ROW == rc ){
        /* if we have a callback... */
        if( xCallback ){
          /* allocate space for col name ptr, value ptr, and type */
          int nCol = sqlite3_column_count(pStmt);
          void *pData = sqlite3_malloc(3*nCol*sizeof(const char*) + 1);
          if( !pData ){
            rc = SQLITE_NOMEM;
          }else{
            char **azCols = (char **)pData;      /* Names of result columns */
            char **azVals = &azCols[nCol];       /* Results */
            int *aiTypes = (int *)&azVals[nCol]; /* Result types */
            int i;
            assert(sizeof(int) <= sizeof(char *)); 
            /* save off ptrs to column names */
            for(i=0; i<nCol; i++){
              azCols[i] = pStmt.columnName(i, COLNAME_NAME)
            }
            do{
              /* extract the data and data types */
              for(i=0; i<nCol; i++){
                azVals[i] = (char *)sqlite3_column_text(pStmt, i);
                aiTypes[i] = sqlite3_column_type(pStmt, i);
                if( !azVals[i] && (aiTypes[i]!=SQLITE_NULL) ){
                  rc = SQLITE_NOMEM;
                  break; /* from for */
                }
              } /* end for */

              /* if data and types extracted successfully... */
              if( SQLITE_ROW == rc ){ 
                /* call the supplied callback with the result row data */
                if( xCallback(pArg, nCol, azVals, azCols, aiTypes) ){
                  rc = SQLITE_ABORT;
                }else{
                  rc = pStmt.Step()
                }
              }
            } while( SQLITE_ROW == rc );
            sqlite3_free(pData);
          }
        }else{
          do{
            rc = pStmt.Step()
          } while( rc == SQLITE_ROW );
        }
      }

      /* print usage stats if stats on */
      if( pArg && pArg->statsOn ){
        display_stats(db, pArg, 0);
      }

      /* Finalize the statement just executed. If this fails, save a 
      ** copy of the error message. Otherwise, set zSql to point to the
      ** next statement to execute. */
      rc2 = pStmt.Finalize()
      if( rc!=SQLITE_NOMEM ) rc = rc2;
      if( rc==SQLITE_OK ){
        zSql = zLeftover;
        while( IsSpace(zSql[0]) ) zSql++;
      }else if( pzErrMsg ){
        *pzErrMsg = save_err_msg(db);
      }

      /* clear saved stmt handle */
      if( pArg ){
        pArg->pStmt = NULL;
      }
    }
  } /* end while */

  return rc;
}


/*
** This is a different callback routine used for dumping the database.
** Each row received by this callback consists of a table name,
** the table type ("index" or "table") and SQL to create the table.
** This routine should print text sufficient to recreate the table.
*/
int dump_callback(void *pArg, int nArg, char **azArg, char **azCol){
	int rc;
	const char *zTable;
	const char *zType;
	const char *zSql;
	const char *zPrepStmt = 0;
	struct callback_data *p = (struct callback_data *)pArg;

	UNUSED_PARAMETER(azCol);
	if( nArg!=3 ) return 1;
	zTable = azArg[0];
	zType = azArg[1];
	zSql = azArg[2];

	switch {
	case zTable == "sqlite_sequence":
		zPrepStmt = "DELETE FROM sqlite_sequence;\n"
	case zTable == "sqlite_stat1":
		fprintf(p->out, "ANALYZE sqlite_master;\n")
	case zTable[0:7] == "sqlite_":
		return 0;
	case zSql[0:20] == "CREATE VIRTUAL TABLE":
		if !p.writableSchema {
			fprintf(p->out, "PRAGMA writable_schema=ON;\n")
			p.writableSchema = 1
		}
		fprintf(p.out, "%s\n", sqlite3_mprintf("INSERT INTO sqlite_master(type, name, tbl_name, rootpage, sql) VALUES('table', '%q', '%q', 0, '%q');", zTable, zTable, zSql))
		return 0
	default:
		fprintf(p.out, "%s;\n", zSql)
	}

	if zType == "table" {
		sqlite3_stmt *pTableInfo = 0
		int nRow = 0
   
		zTableInfo := appendText(zTableInfo, "PRAGMA table_info(", 0)
		zTableInfo = appendText(zTableInfo, zTable, '"')
		zTableInfo = appendText(zTableInfo, ");", 0)

		rc = sqlite3_prepare(p.db, zTableInfo, -1, &pTableInfo, 0)
		if rc != SQLITE_OK || pTableInfo == nil {
			return 1
		}

		zSelect := appendText(zSelect, "SELECT 'INSERT INTO ' || ", 0)
		/* Always quote the table name, even if it appears to be pure ascii, in case it is a keyword. Ex:  INSERT INTO "table" ... */
		zTmp := appendText(zTmp, zTable, '"')
		if zTmp != "" {
			zSelect = appendText(zSelect, zTmp, '\'');
		}
		zSelect = appendText(zSelect, " || ' VALUES(' || ", 0)
		rc = pTableInfo.Step()
		for rc == SQLITE_ROW {
			zText := sqlite3_column_text(pTableInfo, 1)
			zSelect = appendText(zSelect, "quote(", 0)
			zSelect = appendText(zSelect, zText, '"')
			if rc = pTableInfo.Step(); rc == SQLITE_ROW {
				zSelect = appendText(zSelect, "), ", 0)
			} else {
				zSelect = appendText(zSelect, ") ", 0)
			}
			nRow++
		}
		if rc = pTableInfo.Finalize(); rc != SQLITE_OK || nRow == 0 {
			return 1
		}
		zSelect = appendText(zSelect, "|| ')' FROM  ", 0)
		zSelect = appendText(zSelect, zTable, '"')

		if rc = run_table_dump_query(p, zSelect, zPrepStmt); rc == SQLITE_CORRUPT {
			zSelect = appendText(zSelect, " ORDER BY rowid DESC", 0)
			run_table_dump_query(p, zSelect, 0)
		}
	}
	return 0
}

/*
** Run zQuery.  Use dump_callback() as the callback routine so that
** the contents of the query are output as SQL statements.
**
** If we get a SQLITE_CORRUPT error, rerun the query after appending
** "ORDER BY rowid DESC" to the end.
*/
int run_schema_dump_query(
  struct callback_data *p, 
  const char *zQuery
){
  int rc;
  char *zErr = 0;
  rc = sqlite3_exec(p->db, zQuery, dump_callback, p, &zErr);
  if( rc==SQLITE_CORRUPT ){
    char *zQ2;
    int len = strlen30(zQuery);
    fprintf(p->out, "/****** CORRUPTION ERROR *******/\n");
    if( zErr ){
      fprintf(p->out, "/****** %s ******/\n", zErr);
      sqlite3_free(zErr);
      zErr = 0;
    }
    zQ2 = malloc( len+100 );
    if( zQ2==0 ) return rc;
    sqlite3_snprintf(len+100, zQ2, "%s ORDER BY rowid DESC", zQuery);
    rc = sqlite3_exec(p->db, zQ2, dump_callback, p, &zErr);
    if( rc ){
      fprintf(p->out, "/****** ERROR: %s ******/\n", zErr);
    }else{
      rc = SQLITE_CORRUPT;
    }
    sqlite3_free(zErr);
    free(zQ2);
  }
  return rc;
}

/*
** Text of a help message
*/
char zHelp[] =
  ".backup ?DB? FILE      Backup DB (default \"main\") to FILE\n"
  ".bail ON|OFF           Stop after hitting an error.  Default OFF\n"
  ".databases             List names and files of attached databases\n"
  ".dump ?TABLE? ...      Dump the database in an SQL text format\n"
  "                         If TABLE specified, only dump tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".echo ON|OFF           Turn command echo on or off\n"
  ".exit                  Exit this program\n"
  ".explain ?ON|OFF?      Turn output mode suitable for EXPLAIN on or off.\n"
  "                         With no args, it turns EXPLAIN on.\n"
  ".header(s) ON|OFF      Turn display of headers on or off\n"
  ".help                  Show this message\n"
  ".import FILE TABLE     Import data from FILE into TABLE\n"
  ".indices ?TABLE?       Show names of all indices\n"
  "                         If TABLE specified, only show indices for tables\n"
  "                         matching LIKE pattern TABLE.\n"
#ifndef SQLITE_OMIT_LOAD_EXTENSION
  ".load FILE ?ENTRY?     Load an extension library\n"
#endif
  ".log FILE|off          Turn logging on or off.  FILE can be stderr/stdout\n"
  ".mode MODE ?TABLE?     Set output mode where MODE is one of:\n"
  "                         csv      Comma-separated values\n"
  "                         column   Left-aligned columns.  (See .width)\n"
  "                         html     HTML <table> code\n"
  "                         insert   SQL insert statements for TABLE\n"
  "                         line     One value per line\n"
  "                         list     Values delimited by .separator string\n"
  "                         tabs     Tab-separated values\n"
  "                         tcl      TCL list elements\n"
  ".nullvalue STRING      Use STRING in place of NULL values\n"
  ".output FILENAME       Send output to FILENAME\n"
  ".output stdout         Send output to the screen\n"
  ".print STRING...       Print literal STRING\n"
  ".prompt MAIN CONTINUE  Replace the standard prompts\n"
  ".quit                  Exit this program\n"
  ".read FILENAME         Execute SQL in FILENAME\n"
  ".restore ?DB? FILE     Restore content of DB (default \"main\") from FILE\n"
  ".schema ?TABLE?        Show the CREATE statements\n"
  "                         If TABLE specified, only show tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".separator STRING      Change separator used by output mode and .import\n"
  ".show                  Show the current values for various settings\n"
  ".stats ON|OFF          Turn stats on or off\n"
  ".tables ?TABLE?        List names of tables\n"
  "                         If TABLE specified, only list tables matching\n"
  "                         LIKE pattern TABLE.\n"
  ".timeout MS            Try opening locked tables for MS milliseconds\n"
  ".trace FILE|off        Output each SQL statement as it is run\n"
  ".vfsname ?AUX?         Print the name of the VFS stack\n"
  ".width NUM1 NUM2 ...   Set column widths for \"column\" mode\n"
;

char zTimerHelp[] =
  ".timer ON|OFF          Turn the CPU timer measurement on or off\n"
;

/* Forward reference */
int process_input(struct callback_data *p, FILE *in);

/*
** Make sure the database is open.  If it is not, then open it.  If
** the database fails to open, print an error message and exit.
*/
void open_db(struct callback_data *p){
	if p.db == nil {
		sqlite3_initialize();
		sqlite3_open(p.zDbFilename, &p.db)
		db = p.db
		if db != nil && sqlite3_errcode(db) == SQLITE_OK {
			db.CreateFunction("shellstatic", 0, 0, shellstaticFunc, nil, nil, nil)
		}
		if db == nil || sqlite3_errcode(db) != SQLITE_OK {
			fprintf(stderr,"Error: unable to open database \"%s\": %s\n", p.zDbFilename, sqlite3_errmsg(db))
			exit(1)
		}
#ifndef SQLITE_OMIT_LOAD_EXTENSION
		sqlite3_enable_load_extension(p.db, 1)
#endif
	}
}

/*
** Do C-language style dequoting.
**
**    \t    -> tab
**    \n    -> newline
**    \r    -> carriage return
**    \NNN  -> ascii character NNN in octal
**    \\    -> backslash
*/
void resolve_backslashes(char *z){
  int i, j;
  char c;
  for(i=j=0; (c = z[i])!=0; i++, j++){
    if( c=='\\' ){
      c = z[++i];
      if( c=='n' ){
        c = '\n';
      }else if( c=='t' ){
        c = '\t';
      }else if( c=='r' ){
        c = '\r';
      }else if( c>='0' && c<='7' ){
        c -= '0';
        if( z[i+1]>='0' && z[i+1]<='7' ){
          i++;
          c = (c<<3) + z[i] - '0';
          if( z[i+1]>='0' && z[i+1]<='7' ){
            i++;
            c = (c<<3) + z[i] - '0';
          }
        }
      }
    }
    z[j] = c;
  }
  z[j] = 0;
}

/*
** Interpret zArg as a boolean value.  Return either 0 or 1.
*/
int booleanValue(char *zArg){
  int i;
  for(i=0; zArg[i]>='0' && zArg[i]<='9'; i++){}
  if( i>0 && zArg[i]==0 ) return atoi(zArg);
  if CaseInsensitiveComparison(zArg, "on") == 0 || CaseInsensitiveComparison(zArg, "yes") == 0 {
    return 1;
  }
  if CaseInsensitiveComparison(zArg, "off") == 0 || CaseInsensitiveComparison(zArg, "no") == 0 {
    return 0;
  }
  fprintf(stderr, "ERROR: Not a boolean value: \"%s\". Assuming \"no\".\n",
          zArg);
  return 0;
}

/*
** Interpret zArg as an integer value, possibly with suffixes.
*/
sqlite3_int64 integerValue(const char *zArg){
  sqlite3_int64 v = 0;
  const struct { char *zSuffix; int iMult; } aMult[] = {
    { "KiB", 1024 },
    { "MiB", 1024*1024 },
    { "GiB", 1024*1024*1024 },
    { "KB",  1000 },
    { "MB",  1000000 },
    { "GB",  1000000000 },
    { "K",   1000 },
    { "M",   1000000 },
    { "G",   1000000000 },
  };
  int i;
  int isNeg = 0;
  if( zArg[0]=='-' ){
    isNeg = 1;
    zArg++;
  }else if( zArg[0]=='+' ){
    zArg++;
  }
  while( isdigit(zArg[0]) ){
    v = v*10 + zArg[0] - '0';
    zArg++;
  }
  for(i=0; i<sizeof(aMult)/sizeof(aMult[0]); i++){
    if CaseInsensitiveComparison(aMult[i].zSuffix, zArg) == 0 {
      v *= aMult[i].iMult;
      break;
    }
  }
  return isNeg? -v : v;
}

/*
** Close an output file, assuming it is not stderr or stdout
*/
void output_file_close(FILE *f){
  if( f && f!=stdout && f!=stderr ) fclose(f);
}

/*
** Try to open an output file.   The names "stdout" and "stderr" are
** recognized and do the right thing.  NULL is returned if the output 
** filename is "off".
*/
func output_file_open(filename string) (f *FILE) {
	switch filename {
	case "stdout":
		f = stdout
	case "stderr":
		f = stderr
	case "off":
		f = nil
	}else{
		if f = fopen(filename, "wb"); f == nil {
			fprintf(stderr, "Error: cannot open \"%s\"\n", filename)
		}
	}
	return f
}

/*
** A routine for handling output from sqlite3_trace().
*/
func sql_trace_callback(pArg interface{}, z string) {
	if f := (*FILE)(pArg); f != nil {
		fprintf(f, "%s\n", z)
	}
}

/*
** A no-op routine that runs with the ".breakpoint" doc-command.  This is
** a useful spot to set a debugger breakpoint.
*/
void test_breakpoint(void){
  int nCall = 0;
  nCall++;
}

/*
** If an input line begins with "." then invoke this routine to
** process that line.
**
** Return 1 on error, 2 to exit, and 0 otherwise.
*/
int do_meta_command(char *zLine, struct callback_data *p){
  int i = 1;
  int nArg = 0;
  int n, c;
  int rc = 0;
  char *azArg[50];

  /* Parse the input line into tokens.
  */
  while( zLine[i] && nArg<ArraySize(azArg) ){
    while( IsSpace(zLine[i]) ){ i++; }
    if( zLine[i]==0 ) break;
    if( zLine[i]=='\'' || zLine[i]=='"' ){
      int delim = zLine[i++];
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && zLine[i]!=delim ){ i++; }
      if( zLine[i]==delim ){
        zLine[i++] = 0;
      }
      if( delim=='"' ) resolve_backslashes(azArg[nArg-1]);
    }else{
      azArg[nArg++] = &zLine[i];
      while( zLine[i] && !IsSpace(zLine[i]) ){ i++; }
      if( zLine[i] ) zLine[i++] = 0;
      resolve_backslashes(azArg[nArg-1]);
    }
  }

  /* Process the input line.
  */
  if( nArg==0 ) return 0; /* no tokens, no error */
  n = strlen30(azArg[0]);
  c = azArg[0][0];
  if c == 'b' && n >= 3 && azArg[0][:n] == "backup" {
    const char *zDestFile = 0;
    const char *zDb = 0;
    const char *zKey = 0;
    sqlite3 *pDest;
    sqlite3_backup *pBackup;
    int j;
    for(j=1; j<nArg; j++){
      const char *z = azArg[j];
	  switch {
      case z[0] == '-':
        while( z[0]=='-' ) z++;
        if z == "key" && j < nArg - 1 {
			zKey = azArg[++j]
        } else {
			fprintf(stderr, "unknown option: %s\n", azArg[j])
			return 1
        }
      case zDestFile == "":
        zDestFile = azArg[j]
      case zDb == "":
        zDb = zDestFile
        zDestFile = azArg[j]
      default:
        fprintf(stderr, "too many arguments to .backup\n")
        return 1
      }
    }
    if( zDestFile==0 ){
      fprintf(stderr, "missing FILENAME argument on .backup\n");
      return 1;
    }
    if( zDb==0 ) zDb = "main";
    rc = sqlite3_open(zDestFile, &pDest);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zDestFile);
      sqlite3_close(pDest);
      return 1;
    }
#ifdef SQLITE_HAS_CODEC
    sqlite3_key(pDest, zKey, (int)len(zKey));
#else
    (void)zKey;
#endif
    open_db(p);
    pBackup = sqlite3_backup_init(pDest, "main", p->db, zDb);
    if( pBackup==0 ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(pDest));
      sqlite3_close(pDest);
      return 1;
    }
    while(  (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK ){}
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = 0;
    }else{
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(pDest));
      rc = 1;
    }
    sqlite3_close(pDest);
  }else

  if c == 'b' && n >= 3 && azArg[0][:n] == "bail" && nArg > 1 && nArg < 3 {
    bail_on_error = booleanValue(azArg[1]);
  }else

  /* The undocumented ".breakpoint" command causes a call to the no-op
  ** routine named test_breakpoint().
  */
  if c == 'b' && n >= 3 && azArg[0][:n] == "breakpoint" {
    test_breakpoint();
  }else

  if c == 'd' && n > 1 && azArg[0][:n] == "databases" && nArg == 1 {
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 1;
    data.mode = MODE_Column;
    data.colWidth[0] = 3;
    data.colWidth[1] = 15;
    data.colWidth[2] = 58;
    data.cnt = 0;
    sqlite3_exec(p->db, "PRAGMA database_list; ", callback, &data, &zErrMsg);
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }
  }else

  if c == 'd' && azArg[0][:n] == "dump" && nArg < 3 {
    open_db(p);
    /* When playing back a "dump", the content might appear in an order
    ** which causes immediate foreign key constraints to be violated.
    ** So disable foreign-key constraint enforcement to prevent problems. */
    fprintf(p->out, "PRAGMA foreign_keys=OFF;\n");
    fprintf(p->out, "BEGIN TRANSACTION;\n");
    p->writableSchema = 0;
    sqlite3_exec(p->db, "SAVEPOINT dump; PRAGMA writable_schema=ON", 0, 0, 0);
    p->nErr = 0;
    if( nArg==1 ){
      run_schema_dump_query(p, 
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE sql NOT NULL AND type=='table' AND name!='sqlite_sequence'"
      );
      run_schema_dump_query(p, 
        "SELECT name, type, sql FROM sqlite_master "
        "WHERE name=='sqlite_sequence'"
      );
      run_table_dump_query(p,
        "SELECT sql FROM sqlite_master "
        "WHERE sql NOT NULL AND type IN ('index','trigger','view')", 0
      );
    }else{
      int i;
      for(i=1; i<nArg; i++){
        zShellStatic = azArg[i];
        run_schema_dump_query(p,
          "SELECT name, type, sql FROM sqlite_master "
          "WHERE tbl_name LIKE shellstatic() AND type=='table'"
          "  AND sql NOT NULL");
        run_table_dump_query(p,
          "SELECT sql FROM sqlite_master "
          "WHERE sql NOT NULL"
          "  AND type IN ('index','trigger','view')"
          "  AND tbl_name LIKE shellstatic()", 0
        );
        zShellStatic = 0;
      }
    }
    if( p->writableSchema ){
      fprintf(p->out, "PRAGMA writable_schema=OFF;\n");
      p->writableSchema = 0;
    }
    sqlite3_exec(p->db, "PRAGMA writable_schema=OFF;", 0, 0, 0);
    sqlite3_exec(p->db, "RELEASE dump;", 0, 0, 0);
    fprintf(p->out, p->nErr ? "ROLLBACK; -- due to errors\n" : "COMMIT;\n");
  }else

  if c == 'e' && azArg[0][:n] == "echo" && nArg > 1 && nArg < 3 {
    p->echoOn = booleanValue(azArg[1]);
  }else

  if c == 'e' && azArg[0][:n] == "exit" {
    if( nArg>1 && (rc = atoi(azArg[1]))!=0 ) exit(rc);
    rc = 2;
  }else

  if c == 'e' && azArg[0][:n] == "explain" && nArg < 3 {
    int val = nArg>=2 ? booleanValue(azArg[1]) : 1;
    if(val == 1) {
      if(!p->explainPrev.valid) {
        p->explainPrev.valid = 1;
        p->explainPrev.mode = p->mode;
        p->explainPrev.showHeader = p->showHeader;
        memcpy(p->explainPrev.colWidth,p->colWidth,sizeof(p->colWidth));
      }
      /* We could put this code under the !p->explainValid
      ** condition so that it does not execute if we are already in
      ** explain mode. However, always executing it allows us an easy
      ** was to reset to explain mode in case the user previously
      ** did an .explain followed by a .width, .mode or .header
      ** command.
      */
      p->mode = MODE_Explain;
      p->showHeader = 1;
      memset(p->colWidth,0,ArraySize(p->colWidth));
      p->colWidth[0] = 4;                  /* addr */
      p->colWidth[1] = 13;                 /* opcode */
      p->colWidth[2] = 4;                  /* P1 */
      p->colWidth[3] = 4;                  /* P2 */
      p->colWidth[4] = 4;                  /* P3 */
      p->colWidth[5] = 13;                 /* P4 */
      p->colWidth[6] = 2;                  /* P5 */
      p->colWidth[7] = 13;                  /* Comment */
    }else if (p->explainPrev.valid) {
      p->explainPrev.valid = 0;
      p->mode = p->explainPrev.mode;
      p->showHeader = p->explainPrev.showHeader;
      memcpy(p->colWidth,p->explainPrev.colWidth,sizeof(p->colWidth));
    }
  }else

  if c == 'h' && (azArg[0][:n] == "header" || azArg[0][:n] == "headers") && nArg > 1 && nArg < 3 {
    p->showHeader = booleanValue(azArg[1]);
  }else

  if c == 'h' && azArg[0][:n] == "help" {
    fprintf(stderr,"%s",zHelp);
    if( HAS_TIMER ){
      fprintf(stderr,"%s",zTimerHelp);
    }
  }else

  if c == 'i' && azArg[0][:n] == "import" && nArg == 3 {
    char *zTable = azArg[2];    /* Insert data into this table */
    char *zFile = azArg[1];     /* The file from which to extract data */
    sqlite3_stmt *pStmt = NULL; /* A statement */
    int nCol;                   /* Number of columns in the table */
    int nByte;                  /* Number of bytes in an SQL string */
    int i, j;                   /* Loop counters */
    int nSep;                   /* Number of bytes in p->separator[] */
    char *zSql;                 /* An SQL statement */
    char *zLine;                /* A single line of input from the file */
    char **azCol;               /* zLine[] broken up into columns */
    char *zCommit;              /* How to commit changes */   
    FILE *in;                   /* The input file */
    int lineno = 0;             /* Line number of input file */

    open_db(p);
    nSep = strlen30(p->separator);
    if( nSep==0 ){
      fprintf(stderr, "Error: non-null separator required for import\n");
      return 1;
    }
    zSql = sqlite3_mprintf("SELECT * FROM %s", zTable);
    if( zSql==0 ){
      fprintf(stderr, "Error: out of memory\n");
      return 1;
    }
    nByte = strlen30(zSql);
    rc = sqlite3_prepare(p->db, zSql, -1, &pStmt, 0);
    sqlite3_free(zSql);
    if( rc ){
      pStmt.Finalize()
      fprintf(stderr,"Error: %s\n", sqlite3_errmsg(db));
      return 1;
    }
    nCol = sqlite3_column_count(pStmt);
    pStmt.Finalize()
    pStmt = nil
    if( nCol==0 ) return 0; /* no columns, no error */
    zSql = malloc( nByte + 20 + nCol*2 );
    if( zSql==0 ){
      fprintf(stderr, "Error: out of memory\n");
      return 1;
    }
    sqlite3_snprintf(nByte+20, zSql, "INSERT INTO %s VALUES(?", zTable);
    j = strlen30(zSql);
    for(i=1; i<nCol; i++){
      zSql[j++] = ',';
      zSql[j++] = '?';
    }
    zSql[j++] = ')';
    zSql[j] = 0;
    rc = sqlite3_prepare(p->db, zSql, -1, &pStmt, 0);
    free(zSql);
    if( rc ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(db));
	  pStmt.Finalize()
      return 1;
    }
    in = fopen(zFile, "rb");
    if( in==0 ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zFile);
      pStmt.Finalize()
      return 1;
    }
    azCol = malloc( sizeof(azCol[0])*(nCol+1) );
    if( azCol==0 ){
      fprintf(stderr, "Error: out of memory\n");
      fclose(in);
      pStmt.Finalize()
      return 1;
    }
    sqlite3_exec(p->db, "BEGIN", 0, 0, 0);
    zCommit = "COMMIT";
    while( (zLine = local_getline(0, in, 1))!=0 ){
      char *z, c;
      int inQuote = 0;
      lineno++;
      azCol[0] = zLine;
      for(i=0, z=zLine; (c = *z)!=0; z++){
        if( c=='"' ) inQuote = !inQuote;
        if( c=='\n' ) lineno++;
        if !inQuote && c == p.separator[0] && z[:nSep] == p.separator {
          *z = 0;
          i++;
          if( i<nCol ){
            azCol[i] = &z[nSep];
            z += nSep-1;
          }
        }
      } /* end for */
      *z = 0;
      if( i+1!=nCol ){
        fprintf(stderr,
                "Error: %s line %d: expected %d columns of data but found %d\n",
                zFile, lineno, nCol, i+1);
        zCommit = "ROLLBACK";
        free(zLine);
        rc = 1;
        break; /* from while */
      }
      for(i=0; i<nCol; i++){
        if( azCol[i][0]=='"' ){
          int k;
          for(z=azCol[i], j=1, k=0; z[j]; j++){
            if( z[j]=='"' ){ j++; if( z[j]==0 ) break; }
            z[k++] = z[j];
          }
          z[k] = 0;
        }
        sqlite3_bind_text(pStmt, i+1, azCol[i], -1, SQLITE_STATIC);
      }
      pStmt.Step()
      rc = pStmt.Reset()
      free(zLine);
      if( rc!=SQLITE_OK ){
        fprintf(stderr,"Error: %s\n", sqlite3_errmsg(db));
        zCommit = "ROLLBACK";
        rc = 1;
        break; /* from while */
      }
    } /* end while */
    free(azCol);
    fclose(in);
    pStmt.Finalize()
    sqlite3_exec(p->db, zCommit, 0, 0, 0);
  }else

  if c == 'i' && azArg[0][:n] == "indices" && nArg < 3 {
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_List;
    if( nArg==1 ){
      rc = sqlite3_exec(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND name NOT LIKE 'sqlite_%' "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type='index' "
        "ORDER BY 1",
        callback, &data, &zErrMsg
      );
    }else{
      zShellStatic = azArg[1];
      rc = sqlite3_exec(p->db,
        "SELECT name FROM sqlite_master "
        "WHERE type='index' AND tbl_name LIKE shellstatic() "
        "UNION ALL "
        "SELECT name FROM sqlite_temp_master "
        "WHERE type='index' AND tbl_name LIKE shellstatic() "
        "ORDER BY 1",
        callback, &data, &zErrMsg
      );
      zShellStatic = 0;
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }else if( rc != SQLITE_OK ){
      fprintf(stderr,"Error: querying sqlite_master and sqlite_temp_master\n");
      rc = 1;
    }
  }else

#ifndef SQLITE_OMIT_LOAD_EXTENSION
  if c == 'l' && azArg[0][:n] == "load" && nArg >= 2 {
    const char *zFile, *zProc;
    char *zErrMsg = 0;
    zFile = azArg[1];
    zProc = nArg>=3 ? azArg[2] : 0;
    open_db(p);
    rc = sqlite3_load_extension(p->db, zFile, zProc, &zErrMsg);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }
  }else
#endif

  if c == 'l' && azArg[0][:n] == "log" && nArg >= 2 {
    const char *zFile = azArg[1];
    output_file_close(p->pLog);
    p->pLog = output_file_open(zFile);
  }else

  if c == 'm' && azArg[0][:n] == "mode" && nArg == 2 {
    int n2 = strlen30(azArg[1]);
    if (n2 == 4 && azArg[1][:n2] == "line") || (n2 == 5 && azArg[1][:n2] == "lines") {
      p->mode = MODE_Line;
    }else if (n2 == 6 && azArg[1][:n2] == "column") || (n2 == 7 && azArg[1][:n2] == "columns") {
      p->mode = MODE_Column;
    }else if n2 == 4 && azArg[1][:n2] == "list" {
      p->mode = MODE_List;
    }else if n2 == 4 && azArg[1][:n2] == "html" {
      p->mode = MODE_Html;
    }else if n2 == 3 && azArg[1][:n2] == "tcl" {
      p->mode = MODE_Tcl;
      sqlite3_snprintf(sizeof(p->separator), p->separator, " ");
    }else if n2 == 3 && azArg[1][:n2] == "csv" {
      p->mode = MODE_Csv;
      sqlite3_snprintf(sizeof(p->separator), p->separator, ",");
    }else if n2 == 4 && azArg[1][:n2] == "tabs" {
      p->mode = MODE_List;
      sqlite3_snprintf(sizeof(p->separator), p->separator, "\t");
    }else if n2 == 6 && azArg[1][:n2] == "insert" {
      p->mode = MODE_Insert;
      set_table_name(p, "table");
    }else {
      fprintf(stderr,"Error: mode should be one of: "
         "column csv html insert line list tabs tcl\n");
      rc = 1;
    }
  }else

  if c == 'm' && azArg[0][:n] == "mode" && nArg == 3 {
    int n2 = strlen30(azArg[1]);
    if n2 == 6 && azArg[1][:n2] == "insert" {
      p->mode = MODE_Insert;
      set_table_name(p, azArg[2]);
    }else {
      fprintf(stderr, "Error: invalid arguments: "
        " \"%s\". Enter \".help\" for help\n", azArg[2]);
      rc = 1;
    }
  }else

  if c == 'n' && azArg[0][:n] == "nullvalue" && nArg == 2 {
    sqlite3_snprintf(sizeof(p->nullvalue), p->nullvalue, "%.*s", (int)ArraySize(p->nullvalue)-1, azArg[1])
  }else

  if c == 'o' && azArg[0][:n] == "output" && nArg == 2 {
    if( p->outfile[0]=='|' ){
      pclose(p->out);
    }else{
      output_file_close(p->out);
    }
    p->outfile[0] = 0;
    if( azArg[1][0]=='|' ){
      p->out = popen(&azArg[1][1], "w");
      if( p->out==0 ){
        fprintf(stderr,"Error: cannot open pipe \"%s\"\n", &azArg[1][1]);
        p->out = stdout;
        rc = 1;
      }else{
        sqlite3_snprintf(sizeof(p->outfile), p->outfile, "%s", azArg[1]);
      }
    }else{
      p->out = output_file_open(azArg[1]);
      if p.out == 0 {
        if azArg[1] == "off" {
          fprintf(stderr,"Error: cannot write to \"%s\"\n", azArg[1])
        }
        p.out = stdout
        rc = 1
      } else {
        sqlite3_snprintf(sizeof(p.outfile), p.outfile, "%s", azArg[1])
      }
    }
  }else

  if c == 'p' && n >= 3 && azArg[0][:n] == "print" {
    int i;
    for(i=1; i<nArg; i++){
      if( i>1 ) fprintf(p->out, " ");
      fprintf(p->out, "%s", azArg[i]);
    }
    fprintf(p->out, "\n");
  }else

  if c == 'p' && azArg[0][:n] == "prompt" && (nArg == 2 || nArg == 3) {
    if( nArg >= 2) {
      strncpy(mainPrompt,azArg[1],(int)ArraySize(mainPrompt)-1);
    }
    if( nArg >= 3) {
      strncpy(continuePrompt,azArg[2],(int)ArraySize(continuePrompt)-1);
    }
  }else

  if c == 'q' && azArg[0][:n] == "quit" && nArg == 1 {
    rc = 2;
  }else

  if c == 'r' && n >= 3 && azArg[0][:n] == "read" && nArg == 2 {
    FILE *alt = fopen(azArg[1], "rb");
    if( alt==0 ){
      fprintf(stderr,"Error: cannot open \"%s\"\n", azArg[1]);
      rc = 1;
    }else{
      rc = process_input(p, alt);
      fclose(alt);
    }
  }else

  if c == 'r' && n >= 3 && azArg[0][:n] == "restore" && nArg > 1 && nArg < 4 {
    const char *zSrcFile;
    const char *zDb;
    sqlite3 *pSrc;
    sqlite3_backup *pBackup;
    int nTimeout = 0;

    if( nArg==2 ){
      zSrcFile = azArg[1];
      zDb = "main";
    }else{
      zSrcFile = azArg[2];
      zDb = azArg[1];
    }
    rc = sqlite3_open(zSrcFile, &pSrc);
    if( rc!=SQLITE_OK ){
      fprintf(stderr, "Error: cannot open \"%s\"\n", zSrcFile);
      sqlite3_close(pSrc);
      return 1;
    }
    open_db(p);
    pBackup = sqlite3_backup_init(p->db, zDb, pSrc, "main");
    if( pBackup==0 ){
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(p->db));
      sqlite3_close(pSrc);
      return 1;
    }
    while( (rc = sqlite3_backup_step(pBackup,100))==SQLITE_OK
          || rc==SQLITE_BUSY  ){
      if( rc==SQLITE_BUSY ){
        if( nTimeout++ >= 3 ) break;
        sqlite3_sleep(100);
      }
    }
    sqlite3_backup_finish(pBackup);
    if( rc==SQLITE_DONE ){
      rc = 0;
    }else if( rc==SQLITE_BUSY || rc==SQLITE_LOCKED ){
      fprintf(stderr, "Error: source database is busy\n");
      rc = 1;
    }else{
      fprintf(stderr, "Error: %s\n", sqlite3_errmsg(p->db));
      rc = 1;
    }
    sqlite3_close(pSrc);
  }else

  if c == 's' && azArg[0][:n] == "schema" && nArg < 3 {
    struct callback_data data;
    char *zErrMsg = 0;
    open_db(p);
    memcpy(&data, p, sizeof(data));
    data.showHeader = 0;
    data.mode = MODE_Semi;
    if( nArg>1 ){
      int i;
      for i = 0; azArg[1][i]; i++ { azArg[1][i] = strings.ToLower(azArg[1][i]) }
	  switch azArg[1] {
      case "sqlite_master":
        callback(&data, 1, []string{ "CREATE TABLE sqlite_master (type text, name text, tbl_name text, rootpage integer sql text)" }, []string{ "sql" })
        rc = SQLITE_OK
      case "sqlite_temp_master":
        callback(&data, 1, []string{ "CREATE TEMP TABLE sqlite_temp_master (type text, name text, tbl_name text, rootpage integer, sql text)" }, []string{ "sql" });
        rc = SQLITE_OK
      default:
        zShellStatic = azArg[1];
        rc = sqlite3_exec(p.db,
          "SELECT sql FROM "
          "  (SELECT sql sql, type type, tbl_name tbl_name, name name, rowid x"
          "     FROM sqlite_master UNION ALL"
          "   SELECT sql, type, tbl_name, name, rowid FROM sqlite_temp_master) "
          "WHERE lower(tbl_name) LIKE shellstatic()"
          "  AND type!='meta' AND sql NOTNULL "
          "ORDER BY rowid",
          callback, &data, &zErrMsg);
        zShellStatic = 0;
      }
    }else{
      rc = sqlite3_exec(p->db,
         "SELECT sql FROM "
         "  (SELECT sql sql, type type, tbl_name tbl_name, name name, rowid x"
         "     FROM sqlite_master UNION ALL"
         "   SELECT sql, type, tbl_name, name, rowid FROM sqlite_temp_master) "
         "WHERE type!='meta' AND sql NOTNULL AND name NOT LIKE 'sqlite_%'"
         "ORDER BY rowid",
         callback, &data, &zErrMsg
      );
    }
    if( zErrMsg ){
      fprintf(stderr,"Error: %s\n", zErrMsg);
      sqlite3_free(zErrMsg);
      rc = 1;
    }else if( rc != SQLITE_OK ){
      fprintf(stderr,"Error: querying schema information\n");
      rc = 1;
    }else{
      rc = 0;
    }
  }else if c == 's' && azArg[0][:n] == "separator" && nArg == 2 {
    sqlite3_snprintf(sizeof(p->separator), p->separator, "%.*s", (int)sizeof(p->separator)-1, azArg[1])
  } else if c == 's' && azArg[0][:n] == "show" && nArg == 1 {
    int i;
    fprintf(p->out,"%9.9s: %s\n","echo", p->echoOn ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","explain", p->explainPrev.valid ? "on" :"off");
    fprintf(p->out,"%9.9s: %s\n","headers", p->showHeader ? "on" : "off");
    fprintf(p->out,"%9.9s: %s\n","mode", modeDescr[p->mode]);
    fprintf(p->out,"%9.9s: ", "nullvalue");
      output_c_string(p->out, p->nullvalue);
      fprintf(p->out, "\n");
    fprintf(p->out,"%9.9s: %s\n","output",
            strlen30(p->outfile) ? p->outfile : "stdout");
    fprintf(p->out,"%9.9s: ", "separator");
      output_c_string(p->out, p->separator);
      fprintf(p->out, "\n");
    fprintf(p->out,"%9.9s: %s\n","stats", p->statsOn ? "on" : "off");
    fprintf(p->out,"%9.9s: ","width");
    for (i=0;i<(int)ArraySize(p->colWidth) && p->colWidth[i] != 0;i++) {
      fprintf(p->out,"%d ",p->colWidth[i]);
    }
    fprintf(p->out,"\n");
  } else if c == 's' && azArg[0][:n] == "stats" && nArg > 1 && nArg < 3 {
    p->statsOn = booleanValue(azArg[1]);
  } else if c == 't' && n > 1 && azArg[0][:n] == "tables" && nArg < 3 {
    sqlite3_stmt *pStmt;
    char **azResult;
    int nRow, nAlloc;
    char *zSql = 0;
    int ii;
    open_db(p);
    pStmt, _, rc = p.db.Prepare_v2("PRAGMA database_list")
    if( rc ) return rc;
    zSql = sqlite3_mprintf(
        "SELECT name FROM sqlite_master"
        " WHERE type IN ('table','view')"
        "   AND name NOT LIKE 'sqlite_%%'"
        "   AND name LIKE ?1");
    while( pStmt.Step() == SQLITE_ROW ){
      switch DbName := sqlite3_column_text(pStmt, 1); DbName {
      case "", "main":
		  continue
	  case "temp":
		  zSql = sqlite3_mprintf(
			  	"%z UNION ALL "
				"SELECT 'temp.' || name FROM sqlite_temp_master"
				" WHERE type IN ('table','view')"
				"   AND name NOT LIKE 'sqlite_%%'"
				"   AND name LIKE ?1", zSql)
	  default:
		  zSql = sqlite3_mprintf(
			  	"%z UNION ALL "
				"SELECT '%q.' || name FROM \"%w\".sqlite_master"
				" WHERE type IN ('table','view')"
				"   AND name NOT LIKE 'sqlite_%%'"
				"   AND name LIKE ?1", zSql, zDbName, zDbName)
	  		}
    }
    pStmt.Finalize()
    zSql = sqlite3_mprintf("%z ORDER BY 1", zSql);
    pStmt, _, rc = p.db.Prepare_v2(zSql)
    sqlite3_free(zSql);
    if( rc ) return rc;
    nRow = nAlloc = 0;
    azResult = 0;
    if( nArg>1 ){
      sqlite3_bind_text(pStmt, 1, azArg[1], -1, SQLITE_TRANSIENT);
    }else{
      sqlite3_bind_text(pStmt, 1, "%", -1, SQLITE_STATIC);
    }
    while( pStmt.Step() == SQLITE_ROW ){
      if( nRow>=nAlloc ){
        char **azNew;
        int n = nAlloc*2 + 10;
        azNew = sqlite3_realloc(azResult, sizeof(azResult[0])*n);
        if( azNew==0 ){
          fprintf(stderr, "Error: out of memory\n");
          break;
        }
        nAlloc = n;
        azResult = azNew;
      }
      azResult[nRow] = sqlite3_mprintf("%s", sqlite3_column_text(pStmt, 0));
      if( azResult[nRow] ) nRow++;
    }
    pStmt.Finalize()
    if( nRow>0 ){
      int len, maxlen = 0;
      int i, j;
      int nPrintCol, nPrintRow;
      for(i=0; i<nRow; i++){
        len = strlen30(azResult[i]);
        if( len>maxlen ) maxlen = len;
      }
      nPrintCol = 80/(maxlen+2);
      if( nPrintCol<1 ) nPrintCol = 1;
      nPrintRow = (nRow + nPrintCol - 1)/nPrintCol;
      for(i=0; i<nPrintRow; i++){
        for(j=i; j<nRow; j+=nPrintRow){
          char *zSp = j<nPrintRow ? "" : "  ";
          fprintf(p->out, "%s%-*s", zSp, maxlen, azResult[j] ? azResult[j] : "");
        }
        fprintf(p->out, "\n");
      }
    }
    for(ii=0; ii<nRow; ii++) sqlite3_free(azResult[ii]);
    sqlite3_free(azResult);
  } else if c == 't' && n >= 8 && azArg[0][:n] == "testctrl" && nArg >= 2 {
    const struct {
       const char *zCtrlName;   /* Name of a test-control option */
       int ctrlCode;            /* Integer code for that option */
    } aCtrl[] = {
      { "prng_save",             SQLITE_TESTCTRL_PRNG_SAVE              },
      { "prng_restore",          SQLITE_TESTCTRL_PRNG_RESTORE           },
      { "prng_reset",            SQLITE_TESTCTRL_PRNG_RESET             },
      { "bitvec_test",           SQLITE_TESTCTRL_BITVEC_TEST            },
      { "fault_install",         SQLITE_TESTCTRL_FAULT_INSTALL          },
      { "benign_malloc_hooks",   SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS    },
      { "pending_byte",          SQLITE_TESTCTRL_PENDING_BYTE           },
      { "assert",                SQLITE_TESTCTRL_ASSERT                 },
      { "always",                SQLITE_TESTCTRL_ALWAYS                 },
      { "reserve",               SQLITE_TESTCTRL_RESERVE                },
      { "optimizations",         SQLITE_TESTCTRL_OPTIMIZATIONS          },
      { "iskeyword",             SQLITE_TESTCTRL_ISKEYWORD              },
      { "scratchmalloc",         SQLITE_TESTCTRL_SCRATCHMALLOC          },
    };
    int testctrl = -1;
    int rc = 0;
    int i, n;
    open_db(p);

    /* convert testctrl text option to value. allow any unique prefix
    ** of the option name, or a numerical value. */
    n = strlen30(azArg[1]);
    for(i=0; i<(int)(sizeof(aCtrl)/sizeof(aCtrl[0])); i++){
      if azArg[1][:n] == aCtrl[i].zCtrlName {
        if( testctrl<0 ){
          testctrl = aCtrl[i].ctrlCode;
        }else{
          fprintf(stderr, "ambiguous option name: \"%s\"\n", azArg[1]);
          testctrl = -1;
          break;
        }
      }
    }
    if( testctrl<0 ) testctrl = atoi(azArg[1]);
    if( (testctrl<SQLITE_TESTCTRL_FIRST) || (testctrl>SQLITE_TESTCTRL_LAST) ){
      fprintf(stderr,"Error: invalid testctrl option: %s\n", azArg[1]);
    }else{
      switch(testctrl){

        /* sqlite3_test_control(int, db, int) */
        case SQLITE_TESTCTRL_OPTIMIZATIONS:
        case SQLITE_TESTCTRL_RESERVE:             
          if( nArg==3 ){
            int opt = (int)strtol(azArg[2], 0, 0);        
            rc = sqlite3_test_control(testctrl, p->db, opt);
            fprintf(p->out, "%d (0x%08x)\n", rc, rc);
          } else {
            fprintf(stderr,"Error: testctrl %s takes a single int option\n",
                    azArg[1]);
          }
          break;

        /* sqlite3_test_control(int) */
        case SQLITE_TESTCTRL_PRNG_SAVE:           
        case SQLITE_TESTCTRL_PRNG_RESTORE:        
        case SQLITE_TESTCTRL_PRNG_RESET:
          if( nArg==2 ){
            rc = sqlite3_test_control(testctrl);
            fprintf(p->out, "%d (0x%08x)\n", rc, rc);
          } else {
            fprintf(stderr,"Error: testctrl %s takes no options\n", azArg[1]);
          }
          break;

        /* sqlite3_test_control(int, uint) */
        case SQLITE_TESTCTRL_PENDING_BYTE:        
          if( nArg==3 ){
            unsigned int opt = (unsigned int)integerValue(azArg[2]);        
            rc = sqlite3_test_control(testctrl, opt);
            fprintf(p->out, "%d (0x%08x)\n", rc, rc);
          } else {
            fprintf(stderr,"Error: testctrl %s takes a single unsigned"
                           " int option\n", azArg[1]);
          }
          break;
          
        /* sqlite3_test_control(int, int) */
        case SQLITE_TESTCTRL_ASSERT:              
        case SQLITE_TESTCTRL_ALWAYS:              
          if( nArg==3 ){
            int opt = atoi(azArg[2]);        
            rc = sqlite3_test_control(testctrl, opt);
            fprintf(p->out, "%d (0x%08x)\n", rc, rc);
          } else {
            fprintf(stderr,"Error: testctrl %s takes a single int option\n",
                            azArg[1]);
          }
          break;

        /* sqlite3_test_control(int, char *) */
#ifdef SQLITE_N_KEYWORD
        case SQLITE_TESTCTRL_ISKEYWORD:           
          if( nArg==3 ){
            const char *opt = azArg[2];        
            rc = sqlite3_test_control(testctrl, opt);
            fprintf(p->out, "%d (0x%08x)\n", rc, rc);
          } else {
            fprintf(stderr,"Error: testctrl %s takes a single char * option\n",
                            azArg[1]);
          }
          break;
#endif

        case SQLITE_TESTCTRL_BITVEC_TEST:         
        case SQLITE_TESTCTRL_FAULT_INSTALL:       
        case SQLITE_TESTCTRL_BENIGN_MALLOC_HOOKS: 
        case SQLITE_TESTCTRL_SCRATCHMALLOC:       
        default:
          fprintf(stderr,"Error: CLI support for testctrl %s not implemented\n",
                  azArg[1]);
          break;
      }
    }
  }else

  if c == 't' && n > 4 && azArg[0][:n] == "timeout" && nArg == 2 {
    open_db(p);
    sqlite3_busy_timeout(p->db, atoi(azArg[1]));
  }else
    
  if HAS_TIMER && c == 't' && n >= 5 && azArg[0][:n] == "timer" && nArg == 2 {
    enableTimer = booleanValue(azArg[1]);
  }else
  
  if c == 't' && azArg[0][:n] == "trace" && nArg > 1 {
    open_db(p);
    output_file_close(p->traceOut);
    p->traceOut = output_file_open(azArg[1]);
    if( p->traceOut==0 ){
      sqlite3_trace(p->db, 0, 0);
    }else{
      sqlite3_trace(p->db, sql_trace_callback, p->traceOut);
    }
  }else

  if c == 'v' && azArg[0][:n] == "version" {
    fprintf(p->out, "SQLite %s %s\n" /*extra-version-info*/,
        sqlite3_libversion(), sqlite3_sourceid());
  }else

  if c == 'v' && azArg[0][:n] == "vfsname" {
    const char *zDbName = nArg==2 ? azArg[1] : "main";
    char *zVfsName = 0;
    if( p->db ){
      sqlite3_file_control(p->db, zDbName, SQLITE_FCNTL_VFSNAME, &zVfsName);
      if( zVfsName ){
        fprintf(p->out, "%s\n", zVfsName);
        sqlite3_free(zVfsName);
      }
    }
  }else

#if defined(SQLITE_DEBUG) && defined(SQLITE_ENABLE_WHERETRACE)
  if c == 'w' && azArg[0][:n] == "wheretrace" {
    extern int sqlite3WhereTrace;
    sqlite3WhereTrace = booleanValue(azArg[1]);
  }else
#endif

  if c == 'w' && azArg[0][:n] == "width" && nArg > 1 {
    int j;
    assert( nArg<=ArraySize(azArg) );
    for(j=1; j<nArg && j<ArraySize(p->colWidth); j++){
      p->colWidth[j-1] = atoi(azArg[j]);
    }
  }else

  {
    fprintf(stderr, "Error: unknown command or invalid arguments: "
      " \"%s\". Enter \".help\" for help\n", azArg[0]);
    rc = 1;
  }

  return rc;
}

/*
** Return TRUE if a semicolon occurs anywhere in the first N characters
** of string z[].
*/
int _contains_semicolon(const char *z, int N){
  int i;
  for(i=0; i<N; i++){  if( z[i]==';' ) return 1; }
  return 0;
}

/*
** Test to see if a line consists entirely of whitespace.
*/
int _all_whitespace(const char *z){
  for(; *z; z++){
    if( IsSpace(z[0]) ) continue;
    if( *z=='/' && z[1]=='*' ){
      z += 2;
      while( *z && (*z!='*' || z[1]!='/') ){ z++; }
      if( *z==0 ) return 0;
      z++;
      continue;
    }
    if( *z=='-' && z[1]=='-' ){
      z += 2;
      while( *z && *z!='\n' ){ z++; }
      if( *z==0 ) return 1;
      continue;
    }
    return 0;
  }
  return 1;
}

/*
** Return TRUE if the line typed in is an SQL command terminator other
** than a semi-colon.  The SQL Server style "go" command is understood
** as is the Oracle "/".
*/
int _is_command_terminator(const char *zLine){
  while( IsSpace(zLine[0]) ){ zLine++; };
  if( zLine[0]=='/' && _all_whitespace(&zLine[1]) ){
    return 1;  /* Oracle */
  }
  if strings.ToLower(zLine[0]) == 'g' && strings.ToLower(zLine[1]) == 'o' && _all_whitespace(&zLine[2]) {
    return 1;  /* SQL Server */
  }
  return 0;
}

/*
** Return true if zSql is a complete SQL statement.  Return false if it
** ends in the middle of a string literal or C-style comment.
*/
int _is_complete(char *zSql, int nSql){
  int rc;
  if( zSql==0 ) return 1;
  zSql[nSql] = ';';
  zSql[nSql+1] = 0;
  rc = sqlite3_complete(zSql);
  zSql[nSql] = 0;
  return rc;
}

/*
** Read input from *in and process it.  If *in==0 then input
** is interactive - the user is typing it it.  Otherwise, input
** is coming from a file or device.  A prompt is issued and history
** is saved only if input is interactive.  An interrupt signal will
** cause this routine to exit immediately, unless input is interactive.
**
** Return the number of errors.
*/
int process_input(struct callback_data *p, FILE *in){
  char *zLine = 0;
  char *zSql = 0;
  int nSql = 0;
  int nSqlPrior = 0;
  char *zErrMsg;
  int rc;
  int errCnt = 0;
  int lineno = 0;
  int startline = 0;

  while( errCnt==0 || !bail_on_error || (in==0 && stdin_is_interactive) ){
    fflush(p->out);
    free(zLine);
    zLine = one_input_line(zSql, in);
    if( zLine==0 ){
      /* End of input */
      if( stdin_is_interactive ) printf("\n");
      break;
    }
    if( seenInterrupt ){
      if( in!=0 ) break;
      seenInterrupt = 0;
    }
    lineno++;
    if( (zSql==0 || zSql[0]==0) && _all_whitespace(zLine) ) continue;
    if( zLine && zLine[0]=='.' && nSql==0 ){
      if( p->echoOn ) printf("%s\n", zLine);
      rc = do_meta_command(zLine, p);
      if( rc==2 ){ /* exit requested */
        break;
      }else if( rc ){
        errCnt++;
      }
      continue;
    }
    if( _is_command_terminator(zLine) && _is_complete(zSql, nSql) ){
      memcpy(zLine,";",2);
    }
    nSqlPrior = nSql;
    if( zSql==0 ){
      int i;
      for(i=0; zLine[i] && IsSpace(zLine[i]); i++){}
      if( zLine[i]!=0 ){
        nSql = strlen30(zLine);
        zSql = malloc( nSql+3 );
        if( zSql==0 ){
          fprintf(stderr, "Error: out of memory\n");
          exit(1);
        }
        memcpy(zSql, zLine, nSql+1);
        startline = lineno;
      }
    }else{
      int len = strlen30(zLine);
      zSql = realloc( zSql, nSql + len + 4 );
      if( zSql==0 ){
        fprintf(stderr,"Error: out of memory\n");
        exit(1);
      }
      zSql[nSql++] = '\n';
      memcpy(&zSql[nSql], zLine, len+1);
      nSql += len;
    }
    if( zSql && _contains_semicolon(&zSql[nSqlPrior], nSql-nSqlPrior)
                && sqlite3_complete(zSql) ){
      p->cnt = 0;
      open_db(p);
      BEGIN_TIMER;
      rc = shell_exec(p->db, zSql, shell_callback, p, &zErrMsg);
      END_TIMER;
      if( rc || zErrMsg ){
        char zPrefix[100];
        if( in!=0 || !stdin_is_interactive ){
          sqlite3_snprintf(sizeof(zPrefix), zPrefix, 
                           "Error: near line %d:", startline);
        }else{
          sqlite3_snprintf(sizeof(zPrefix), zPrefix, "Error:");
        }
        if( zErrMsg!=0 ){
          fprintf(stderr, "%s %s\n", zPrefix, zErrMsg);
          sqlite3_free(zErrMsg);
          zErrMsg = 0;
        }else{
          fprintf(stderr, "%s %s\n", zPrefix, sqlite3_errmsg(p->db));
        }
        errCnt++;
      }
      free(zSql);
      zSql = 0;
      nSql = 0;
    }else if( zSql && _all_whitespace(zSql) ){
      free(zSql);
      zSql = 0;
      nSql = 0;
    }
  }
  if( zSql ){
    if( !_all_whitespace(zSql) ){
      fprintf(stderr, "Error: incomplete SQL: %s\n", zSql);
    }
    free(zSql);
  }
  free(zLine);
  return errCnt>0;
}

/*
** Return a pathname which is the user's home directory.  A
** 0 return indicates an error of some kind.
*/
char *find_home_dir(void){
  char *home_dir = NULL;
  if( home_dir ) return home_dir;

#if && !defined(__RTP__) && !defined(_WRS_KERNEL)
  {
    struct passwd *pwent;
    uid_t uid = getuid();
    if( (pwent=getpwuid(uid)) != NULL) {
      home_dir = pwent->pw_dir;
    }
  }
#endif

  if (!home_dir) {
    home_dir = getenv("HOME");
  }

  if( home_dir ){
    int n = strlen30(home_dir) + 1;
    char *z = malloc( n );
    if( z ) memcpy(z, home_dir, n);
    home_dir = z;
  }

  return home_dir;
}

/*
** Read input from the file given by sqliterc_override.  Or if that
** parameter is NULL, take input from ~/.sqliterc
**
** Returns the number of errors.
*/
int process_sqliterc(
  struct callback_data *p,        /* Configuration data */
  const char *sqliterc_override   /* Name of config file. NULL to use default */
){
  char *home_dir = NULL;
  const char *sqliterc = sqliterc_override;
  char *zBuf = 0;
  FILE *in = NULL;
  int rc = 0;

  if (sqliterc == NULL) {
    home_dir = find_home_dir();
    if( home_dir==0 ){
#if !defined(__RTP__) && !defined(_WRS_KERNEL)
      fprintf(stderr,"%s: Error: cannot locate your home directory\n", Argv0);
#endif
      return 1;
    }
    sqlite3_initialize();
    zBuf = sqlite3_mprintf("%s/.sqliterc",home_dir);
    sqliterc = zBuf;
  }
  in = fopen(sqliterc,"rb");
  if( in ){
    if( stdin_is_interactive ){
      fprintf(stderr,"-- Loading resources from %s\n",sqliterc);
    }
    rc = process_input(p,in);
    fclose(in);
  }
  sqlite3_free(zBuf);
  return rc;
}

/*
** Show available command line options
*/
const char zOptions[] = 
  "   -bail                stop after hitting an error\n"
  "   -batch               force batch I/O\n"
  "   -column              set output mode to 'column'\n"
  "   -cmd COMMAND         run \"COMMAND\" before reading stdin\n"
  "   -csv                 set output mode to 'csv'\n"
  "   -echo                print commands before execution\n"
  "   -init FILENAME       read/process named file\n"
  "   -[no]header          turn headers on or off\n"
#if defined(SQLITE_ENABLE_MEMSYS3) || defined(SQLITE_ENABLE_MEMSYS5)
  "   -heap SIZE           Size of heap for memsys3 or memsys5\n"
#endif
  "   -help                show this message\n"
  "   -html                set output mode to HTML\n"
  "   -interactive         force interactive I/O\n"
  "   -line                set output mode to 'line'\n"
  "   -list                set output mode to 'list'\n"
  "   -mmap N              default mmap size set to N\n"
#ifdef SQLITE_ENABLE_MULTIPLEX
  "   -multiplex           enable the multiplexor VFS\n"
#endif
  "   -nullvalue TEXT      set text string for NULL values. Default ''\n"
  "   -separator SEP       set output field separator. Default: '|'\n"
  "   -stats               print memory stats before each finalize\n"
  "   -version             show SQLite version\n"
  "   -vfs NAME            use NAME as the default VFS\n"
#ifdef SQLITE_ENABLE_VFSTRACE
  "   -vfstrace            enable tracing of all VFS calls\n"
#endif
;
void usage(int showDetail){
  fprintf(stderr,
      "Usage: %s [OPTIONS] FILENAME [SQL]\n"  
      "FILENAME is the name of an SQLite database. A new database is created\n"
      "if the file does not previously exist.\n", Argv0);
  if( showDetail ){
    fprintf(stderr, "OPTIONS include:\n%s", zOptions);
  }else{
    fprintf(stderr, "Use the -help option for additional information\n");
  }
  exit(1);
}

/*
** Initialize the state information in data
*/
void main_init(struct callback_data *data) {
  memset(data, 0, sizeof(*data));
  data->mode = MODE_List;
  memcpy(data->separator,"|", 2);
  data->showHeader = 0;
  sqlite3_config(SQLITE_CONFIG_URI, 1);
  sqlite3_config(SQLITE_CONFIG_LOG, shellLog, data);
  sqlite3_snprintf(sizeof(mainPrompt), mainPrompt,"sqlite> ");
  sqlite3_snprintf(sizeof(continuePrompt), continuePrompt,"   ...> ");
  sqlite3_config(SQLITE_CONFIG_SINGLETHREAD);
}

/*
** Get the argument to an --option.  Throw an error and die if no argument
** is available.
*/
char *cmdline_option_value(int argc, char **argv, int i){
	if i == argc {
		fprintf(stderr, "%s: Error: missing argument to %s\n", argv[0], argv[argc - 1])
		exit(1)
	}
	return argv[i]
}

int main(int argc, char **argv){
	char *zErrMsg = 0;
	struct callback_data data;
	const char *zInitFile = 0;
	char *zFirstCmd = 0;
	int i;
	int rc = 0;

	if sqlite3_sourceid() != SQLITE_SOURCE_ID {
		fprintf(stderr, "SQLite header and source version mismatch\n%s\n%s\n", sqlite3_sourceid(), SQLITE_SOURCE_ID)
		exit(1)
	}
	Argv0 = argv[0]
	main_init(&data)
	stdin_is_interactive = isatty(0)

	/* Make sure we have a valid signal handler early, before anything else is done. */
#ifdef SIGINT
	signal(SIGINT, interrupt_handler);
#endif

  /* Do an initial pass through the command-line argument to locate
  ** the name of the database file, the name of the initialization file,
  ** the size of the alternative malloc heap,
  ** and the first command to execute.
  */
  for(i=1; i<argc; i++){
    char *z;
    z = argv[i];
    if( z[0]!='-' ){
      if( data.zDbFilename==0 ){
        data.zDbFilename = z;
        continue;
      }
      if( zFirstCmd==0 ){
        zFirstCmd = z;
        continue;
      }
      fprintf(stderr,"%s: Error: too many options: \"%s\"\n", Argv0, argv[i]);
      fprintf(stderr,"Use -help for a list of options.\n");
      return 1;
    }
    if( z[1]=='-' ) z++;
	switch z {
    case "-separator", "-nullvalue", "-cmd":
      (void)cmdline_option_value(argc, argv, ++i)
    case "-init":
      zInitFile = cmdline_option_value(argc, argv, ++i);
    case "-batch":
      /* Need to check for batch mode here to so we can avoid printing
      ** informational messages (like from process_sqliterc) before 
      ** we do the actual processing of arguments later in a second pass.
      */
      stdin_is_interactive = 0;
    case "-heap":
#if defined(SQLITE_ENABLE_MEMSYS3) || defined(SQLITE_ENABLE_MEMSYS5)
      int j, c;
      const char *zSize;
      sqlite3_int64 szHeap;

      zSize = cmdline_option_value(argc, argv, ++i);
      szHeap = integerValue(zSize);
      if( szHeap>0x7fff0000 ) szHeap = 0x7fff0000;
      sqlite3_config(SQLITE_CONFIG_HEAP, malloc((int)szHeap), (int)szHeap, 64);
#endif
#ifdef SQLITE_ENABLE_VFSTRACE
    case "-vfstrace":
		extern int vfstrace_register(zTraceName, zOldVfsName string, xOut func(string, interface{}) int, pOutArg *interface{}, makeDefault int)
		vfstrace_register("trace", 0, (int(*)(string, void*))fputs, stderr, 1)
#endif
#ifdef SQLITE_ENABLE_MULTIPLEX
    case "-multiplex":
      extern int sqlite3_multiple_initialize(string, int)
      sqlite3_multiplex_initialize(0, 1)
#endif
    case "-mmap":
      sz := integerValue(cmdline_option_value(argc, argv, ++i))
      sqlite3_config(SQLITE_CONFIG_MMAP_SIZE, sz, sz)
    case "-vfs":
      pVfs := sqlite3_vfs_find(cmdline_option_value(argc, argv, ++i))
      if pVfs {
        sqlite3_vfs_register(pVfs, 1)
      }else{
        fprintf(stderr, "no such VFS: \"%s\"\n", argv[i])
        exit(1)
      }
    }
  }
  if( data.zDbFilename==0 ){
#ifndef SQLITE_OMIT_MEMORYDB
    data.zDbFilename = ":memory:";
#else
    fprintf(stderr,"%s: Error: no database filename specified\n", Argv0);
    return 1;
#endif
  }
  data.out = stdout;

  /* Go ahead and open the database file if it already exists.  If the
  ** file does not exist, delay opening it.  This prevents empty database
  ** files from being created if a user mistypes the database name argument
  ** to the sqlite command-line tool.
  */
  if( access(data.zDbFilename, 0)==0 ){
    open_db(&data);
  }

  /* Process the initialization file if there is one.  If no -init option
  ** is given on the command line, look for a file named ~/.sqliterc and
  ** try to process it.
  */
  rc = process_sqliterc(&data,zInitFile);
  if( rc>0 ){
    return rc;
  }

  /* Make a second pass through the command-line argument and set
  ** options.  This second pass is delayed until after the initialization
  ** file is processed so that the command-line arguments will override
  ** settings in the initialization file.
  */
  for(i=1; i<argc; i++){
    char *z = argv[i];
    if( z[0]!='-' ) continue;
    if( z[1]=='-' ){ z++; }
	switch z {
    case "-init":
      i++;
    case "-html":
      data.mode = MODE_Html;
    case "-list":
      data.mode = MODE_List;
    case "-line":
      data.mode = MODE_Line;
    case "-column":
      data.mode = MODE_Column;
    case "-csv":
      data.mode = MODE_Csv;
      memcpy(data.separator,",",2);
    case "-separator":
      sqlite3_snprintf(sizeof(data.separator), data.separator, "%s",cmdline_option_value(argc, argv, ++i))
    case "-nullvalue":
      sqlite3_snprintf(sizeof(data.nullvalue), data.nullvalue, "%s",cmdline_option_value(argc, argv, ++i))
    case "-header":
      data.showHeader = 1
    case "-noheader" {
      data.showHeader = 0
    case "-echo":
      data.echoOn = 1
    case "-stats":
      data.statsOn = 1
    case "-bail":
      bail_on_error = 1;
    case "-version":
      printf("%s %s\n", sqlite3_libversion(), sqlite3_sourceid())
      return 0
    case "-interactive":
      stdin_is_interactive = 1
    case "-batch":
      stdin_is_interactive = 0
    case "-heap":
      i++
    case "-mmap":
      i++
    case "-vfs":
      i++
#ifdef SQLITE_ENABLE_VFSTRACE
    case "-vfstrace":
      i++
#endif
#ifdef SQLITE_ENABLE_MULTIPLEX
    case "-multiplex":
      i++
#endif
    case "-help":
      usage(1)
    case "-cmd":
      if i == argc - 1 {
		  break
	  }
      z = cmdline_option_value(argc, argv, ++i)
      if z[0] == '.' {
        if rc = do_meta_command(z, &data); rc != 0 && bail_on_error {
			return rc == 2 ? 0 : rc
		}
      } else {
        open_db(&data)
        rc = shell_exec(data.db, z, shell_callback, &data, &zErrMsg)
		switch {
        case zErrMsg != "":
          fprintf(stderr,"Error: %s\n", zErrMsg)
          if bail_on_error  {
			  return rc != 0 ? rc : 1
		  }
        case rc != 0:
          fprintf(stderr, "Error: unable to process SQL \"%s\"\n", z)
          if bail_on_error {
			  return rc
		  }
        }
      }
    default:
      fprintf(stderr, "%s: Error: unknown option: %s\n", Argv0, z)
      fprintf(stderr, "Use -help for a list of options.\n")
      return 1
    }
  }

  if( zFirstCmd ){
    /* Run just the command that follows the database name
    */
    if( zFirstCmd[0]=='.' ){
      rc = do_meta_command(zFirstCmd, &data);
      if( rc==2 ) rc = 0;
    }else{
      open_db(&data);
      rc = shell_exec(data.db, zFirstCmd, shell_callback, &data, &zErrMsg);
      if( zErrMsg!=0 ){
        fprintf(stderr,"Error: %s\n", zErrMsg);
        return rc!=0 ? rc : 1;
      }else if( rc!=0 ){
        fprintf(stderr,"Error: unable to process SQL \"%s\"\n", zFirstCmd);
        return rc;
      }
    }
  }else{
    /* Run commands received from standard input
    */
    if( stdin_is_interactive ){
      char *zHome;
      char *zHistory = 0;
      int nHistory;
      printf(
        "SQLite version %s %.19s\n" /*extra-version-info*/
        "Enter \".help\" for instructions\n"
        "Enter SQL statements terminated with a \";\"\n",
        sqlite3_libversion(), sqlite3_sourceid()
      );
      zHome = find_home_dir();
      if( zHome ){
        nHistory = strlen30(zHome) + 20;
        if( (zHistory = malloc(nHistory))!=0 ){
          sqlite3_snprintf(nHistory, zHistory,"%s/.sqlite_history", zHome);
        }
      }
#if defined(HAVE_READLINE) && HAVE_READLINE==1
      if( zHistory ) read_history(zHistory);
#endif
      rc = process_input(&data, 0);
      if( zHistory ){
        stifle_history(100);
        write_history(zHistory);
        free(zHistory);
      }
    }else{
      rc = process_input(&data, stdin);
    }
  }
  set_table_name(&data, 0);
  if( data.db ){
    sqlite3_close(data.db);
  }
  return rc;
}
