package SharedHive;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author hadoop
 */
import java.text.Format;
import java.text.ParseException;

    


/**
 * An immutable class to handle ranges of comparable values, like date ranges
 * and numeric ranges. A demo main() method is provided to show usage (see at
 * bottom).
 *
 * @author Franco Graziosi
 * @see 
 */
public final class Range<E extends Comparable<E>> {

  private final E lowerBound;
  private final E upperBound;
  private final boolean empty;
  public int intrsect_lowerBound;
  public int intrsect_higherBound;
  public int overlp_count=0;
  public String overlapp_type="";
  /**
   * Construct an empty range.
   */
  public Range() {
    empty = true;
    lowerBound = upperBound = null;
  }

  /**
   * Build a range given its bounds.
   * 
   * @param lowerBound
   *          Minimum value or <code>null</code> if unbound on bottom
   *          (left-open).
   * @param upperBound
   *          Maximum value or <code>null</code> if unbound on top (right-open).
   */
  public Range(E lowerBound, E upperBound) {
    this.lowerBound = lowerBound;
    this.upperBound = upperBound;
    this.empty = isBoundConflict();
    intrsect_lowerBound=0;
    intrsect_higherBound=0;
    
    
  }

  /**
   * Construct a range given a String representation and a format to parse bounds.
   * Bounds shall be separated by "..", for example "1..2". Open ranges are also
   * supported, for example "3.." or "..JAN/25/2010".
   * 
   * @param s
   *          The string to be parsed.
   * @param fmt
   *          The format to be used, if, for example it is a data range, you
   *          shall provide a date format.
   * @throws ParseException
   */
  public Range(String s, Format fmt) throws ParseException {
    int i = s.indexOf("..");
    if (i == -1)
      throw new IllegalArgumentException("Bad Range syntax: " + s);
    String sLow = s.substring(0, i).trim();
    String sHi = s.substring(i + 2).trim();
    lowerBound = parseValue(sLow, fmt);
    upperBound = parseValue(sHi, fmt);
    this.empty = isBoundConflict();
    //intrsect_lowerBound=
  }

  /**
   * Determine if <code>value</code> is contained within the range.
   */
  public boolean contains(E value) {
    if (isEmpty())
      return false;
    if (getUpperBound() != null && value.compareTo(getUpperBound()) > 0)
      return false;
    if (getLowerBound() != null && value.compareTo(getLowerBound()) < 0)
      return false;
    return true;
  }

  /**
   * Determine if this range fully contains another range.
   * 
   * @param r
   *          Another range
   * @return true if all values of <code>r</code> are contained within
   *         <code>this</code> interval
   */
  public boolean contains(Range<E> r) {
    if (isEmpty())
      return r.isEmpty();
    else if (r.isEmpty())
      return true;
    if (getLowerBound() != null) {
      if (r.getLowerBound() == null)
        return false;
      else if (r.getLowerBound().compareTo(getLowerBound()) < 0)
        return false;
    }
    if (getUpperBound() != null) {
      if (r.getUpperBound() == null)
        return false;
      else if (r.getUpperBound().compareTo(getUpperBound()) > 0)
        return false;
    }
    return true;
  }

  /**
   * Format the range using a given mask.
   */
  public Object format(Format format) {
    if (isEmpty())
      return "";
    return (getLowerBound() == null ? "" : format.format(getLowerBound()))
        + " .. "
        + (getUpperBound() == null ? "" : format.format(getUpperBound()));
  }

  /**
   * @return lower bound or <code>null</code> if there is no lower bound.
   */
  public E getLowerBound() {
    return lowerBound;
  }

  /**
   * @return upper bound or <code>null</code> if there is no lower bound.
   */
  public E getUpperBound() {
    return upperBound;
  }

  /**
   * Determine how much two ranges intersect (overlap). For example the overlap
   * between range 1..5 and 4..9 is 4..5.
   * 
   * @param other
   *          Another range
   * @return The overlapping range or <code>null</code> if ranges do not
   *         overlap.
   */
  public Range<E> intersect(Range<E> other) {
    if (isEmpty())
      return this;
    else if (other.isEmpty())
      return other;
    E low = max(lowerBound, other.lowerBound, false);
    E high = min(upperBound, other.upperBound, false);
    intrsect_lowerBound=Integer.parseInt(low.toString());
    intrsect_higherBound=Integer.parseInt(high.toString());
    overlp_count=intrsect_higherBound-intrsect_lowerBound;
    
    return new Range<E>(low, high);
  }

  /**
   * Determine if range is bound. A range is bound if it is not empty and both
   * lower and upper bounds are defined.
   */
  public boolean isBound() {
    return !isEmpty() && getLowerBound() != null && getUpperBound() != null;

  }

  private boolean isBoundConflict() {
    if (lowerBound != null && upperBound != null
        && lowerBound.compareTo(upperBound) > 0)
      return true;
    return false;
  }

  /**
   * Determine if range is degenerate. A range is degenerate when it contains a
   * single value.
   */
  public boolean isDegenerate() {
    return isBound() && getLowerBound().equals(getUpperBound());
  }

  /**
   * Determine if the range is empty. An empty range contains no values.
   */
  public boolean isEmpty() {
    return empty;
  }

  /**
   * Determine if lower bound is defined.
   */
  public boolean isLowerBounded() {
    return lowerBound != null;
  }

  /**
   * Determine if two ranges are overlapping.
   */
  public boolean isOverlapping(Range<E> r2) {
    return !intersect(r2).isEmpty();
  }
  
  /**
   * Determine if upper bound is defined.
   */
  public boolean isUpperBounded() {
    return upperBound != null;
  }

  /**
   * Join two ranges and anything between them, for example 1..4 joined with
   * 2..7 give 1..7 and 1..4 joinde with 9..12 gives 1..12.
   * 
   * @param other
   *          the other range
   * @return a range corresponding to the joined ranges.
   */
  public Range<E> join(Range<E> other) {
    if (isEmpty())
      return other;
    else if (other.isEmpty())
      return this;
    E low = min(lowerBound, other.lowerBound, true);
    E high = max(upperBound, other.upperBound, true);
    return new Range<E>(low, high);
  }

  private E max(E x1, E x2, boolean keepUndefined) {
    if (x1 == null)
      return keepUndefined ? x1 : x2;
    else if (x2 == null)
      return keepUndefined ? x2 : x1;
    if (x1.compareTo(x2) >= 0)
      return x1;
    return x2;
  }

  private E min(E x1, E x2, boolean keepUndefined) {
    if (x1 == null)
      return keepUndefined ? x1 : x2;
    else if (x2 == null)
      return keepUndefined ? x2 : x1;
    if (x1.compareTo(x2) <= 0)
      return x1;
    return x2;
  }

  @SuppressWarnings("unchecked")
  private E parseValue(String s, Format fmt) throws ParseException {
    if (s.length() == 0)
      return null;
    E result = (E) fmt.parseObject(s);
    return result;
  }

  public String toString() {
    if (isEmpty())
      return "empty";
    return (getLowerBound() == null ? "" : getLowerBound()) + " .. "
        + (getUpperBound() == null ? "" : getUpperBound());
  }

  // Demonstration code follows
  
  private static class Demo {
    private Range<Integer> threeToSeven = new Range<Integer>(1, 20);

    private void run() {

      Range<Integer> oneToFive = new Range<Integer>(20, 60);
      Range<Integer> oneToEleven = new Range<Integer>(5, 25);
      Range<Integer> upToFifteer = new Range<Integer>(null, 15);
      Range<Integer> fourAndUp = new Range<Integer>(4, null);
      Range<Integer> infiniteRange = new Range<Integer>((Integer) null,
          (Integer) null);
      Range<Integer> emptyRange = new Range<Integer>();
      Range<Integer> degenerate = new Range<Integer>(6, 13);

      show(oneToFive);
      show(oneToEleven);
      show(upToFifteer);
      show(fourAndUp);
      show(infiniteRange);
      show(emptyRange);
      show(degenerate);
      show(threeToSeven);

    }

    private void show(Range<Integer> r2) {
      System.out.println("______________________");
      System.out.println(threeToSeven + " intersect(" + r2 + ") = "
          + threeToSeven.intersect(r2));
      System.out.println(threeToSeven + " isOverlapping(" + r2 + ") = "
          + threeToSeven.isOverlapping(r2));
      System.out.println(threeToSeven + " join(" + r2 + ") = "
          + threeToSeven.join(r2));
      System.out.println("isBound(" + r2 + ") = " + r2.isBound());
      System.out.println("isDegenerate(" + r2 + ") = " + r2.isDegenerate());
    }
  }

  public static void main(String[] args) {
    //(new Demo()).run();
      testoverlap();
    
  }
 public static void testoverlap()
 {
      Range<Integer> Q1 = new Range<Integer>(15, 20);
     
      Range<Integer> Q2 = new Range<Integer>(1, 10);
       Q1.intersect(Q2);
        System.out.println(Q1 + " intersect(" + Q2 + ") = "
          + Q1.intersect(Q2));
        int ol=(Q1.intrsect_higherBound-Q1.intrsect_lowerBound);
        ol=Q1.overlp_count;
         System.out.println("intrsect_lowerBound= "+Q1.intrsect_lowerBound);
         System.out.println("intrsect_higherBound= "+Q1.intrsect_higherBound);
         System.out.println("ovelapping length="+ ol);
      
 }
}

