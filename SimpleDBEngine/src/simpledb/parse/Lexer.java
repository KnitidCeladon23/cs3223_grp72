package simpledb.parse;

import java.util.*;
import java.io.*;

/**
 * The lexical analyzer.
 * @author Edward Sciore
 */
public class Lexer {
   private Collection<String> keywords;
   private Collection<String> aggregations;
   private Collection<String> comparisonOperators;
   private StreamTokenizer tok;
   
   /**
    * Creates a new lexical analyzer for SQL statement s.
    * @param s the SQL statement
    */
   public Lexer(String s) {
      initKeywords();
      initAggregation();
      tok = new StreamTokenizer(new StringReader(s));
      tok.ordinaryChar('.');   //disallow "." in identifiers
      tok.wordChars('_', '_'); //allow "_" in identifiers
      tok.lowerCaseMode(true); //ids and keywords are converted
      nextToken();
   }
   
//Methods to check the status of the current token
   
   /**
    * Returns true if the current token is
    * the specified delimiter character.
    * @param d a character denoting the delimiter
    * @return true if the delimiter is the current token
    */
   public boolean matchDelim(char d) {
      return d == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is an integer.
    * @return true if the current token is an integer
    */
   public boolean matchIntConstant() {
      return tok.ttype == StreamTokenizer.TT_NUMBER;
   }
   
   /**
    * Returns true if the current token is a string.
    * @return true if the current token is a string
    */
   public boolean matchStringConstant() {
      return '\'' == (char)tok.ttype;
   }
   
   /**
    * Returns true if the current token is the specified keyword.
    * @param w the keyword string
    * @return true if that keyword is the current token
    */
   public boolean matchKeyword(String w) {
      return tok.ttype == StreamTokenizer.TT_WORD && tok.sval.equals(w);
   }
   
   /**
    * Returns true if the current token is a legal identifier.
    * @return true if the current token is an identifier
    */
   public boolean matchId() {
      return  tok.ttype==StreamTokenizer.TT_WORD && !keywords.contains(tok.sval);
   }
   
   public boolean matchAggregation() {
      return tok.ttype == StreamTokenizer.TT_WORD && aggregations.contains(tok.sval);
   }
   
//Methods to "eat" the current token
   
   /**
    * Throws an exception if the current token is not the
    * specified delimiter. 
    * Otherwise, moves to the next token.
    * @param d a character denoting the delimiter
    */
   public void eatDelim(char d) {
      if (!matchDelim(d))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an integer. 
    * Otherwise, returns that integer and moves to the next token.
    * @return the integer value of the current token
    */
   public int eatIntConstant() {
      if (!matchIntConstant())
         throw new BadSyntaxException();
      int i = (int) tok.nval;
      nextToken();
      return i;
   }
   
   /**
    * Throws an exception if the current token is not 
    * a string. 
    * Otherwise, returns that string and moves to the next token.
    * @return the string value of the current token
    */
   public String eatStringConstant() {
      if (!matchStringConstant())
         throw new BadSyntaxException();
      String s = tok.sval; //constants are not converted to lower case
      nextToken();
      return s;
   }
   
   /**
    * Throws an exception if the current token is not the
    * specified keyword. 
    * Otherwise, moves to the next token.
    * @param w the keyword string
    */
   public void eatKeyword(String w) {
      if (!matchKeyword(w))
         throw new BadSyntaxException();
      nextToken();
   }
   
   /**
    * Throws an exception if the current token is not 
    * an identifier. 
    * Otherwise, returns the identifier string 
    * and moves to the next token.
    * @return the string value of the current token
    */
   public String eatId() {
      if (!matchId())
         throw new BadSyntaxException();
      String s = tok.sval;
      nextToken();
      return s;
   }
   
   public String eatComOpr() {
       String opr = getComparisonOperator();
       if (!matchComOpr(opr))
           throw new BadSyntaxException();
       nextToken();
       return opr;
   }
   
   public String eatAggregation() {
	   if (!matchAggregation())
	       throw new BadSyntaxException();
	   String aggregation = tok.sval;
	   nextToken();
	       return aggregation;
	   }
   
   public String eatEquality() {
	      String comparator;
	      if(matchDelim('<')){
	         eatDelim('<');
	         if(matchDelim('=')){
	            eatDelim('=');
	            comparator = "<=";
	         } else if(matchDelim('>')){
	            eatDelim('>');
	            comparator = "<>";
	         } else {
	            comparator = "<";
	         }
	      } else if(matchDelim('>')){
	         eatDelim('>');
	         if(matchDelim('=')){
	            eatDelim('=');
	            comparator = ">=";
	         } else {
	            comparator = ">";
	         }
	      } else if(matchDelim('!')){
	         eatDelim('!');
	         // no need to check if the match '='
	         // since after ! must be =
	         eatDelim('=');
	         comparator = "!=";
	      } else if (matchDelim('=')) {
	         eatDelim('=');
	         comparator = "=";
	      } else {
	         throw new BadSyntaxException();
	      }
	      return comparator;
	   }
	   
   
   public boolean matchComOpr(String opr) {
       return comparisonOperators.contains(opr);
   }

   private String getComparisonOperator() {
       try {
           switch(tok.ttype) {
               case '=':
                   return "=";
               case '<':
                   switch(tok.nextToken()) {
                       case '=':
                           return "<=";
                       case '>':
                           return "<>";
                       default:
                           tok.pushBack();
                           return "<";
                   }
               case '>':
                   switch(tok.nextToken()) {
                       case '=':
                           return ">=";
                       default:
                           tok.pushBack();
                           return ">";
                   }
               case '!':
                   switch(tok.nextToken()) {
                       case '=':
                           return "!=";
                       default:
                           tok.pushBack();
                           throw new BadSyntaxException();
                   }
               default:
                   return Character.toString((char) tok.ttype);
           }
       } catch(IOException e) {
           throw new BadSyntaxException();
       }
   }
   
   private void nextToken() {
      try {
         tok.nextToken();
      }
      catch(IOException e) {
         throw new BadSyntaxException();
      }
   }
   
   private void initKeywords() {
      keywords = Arrays.asList("select", "from", "where", "and",
                               "insert", "into", "values", "delete", "update", "set", 
                               "create", "table", "int", "varchar", "view", "as", "index", "on",
                               "order", "by", "using", "hash", "btree", "distinct");
   }
   
   private void initAggregation() {
      aggregations = Arrays.asList("sum", "count", "avg", "min", "max");
   }
}