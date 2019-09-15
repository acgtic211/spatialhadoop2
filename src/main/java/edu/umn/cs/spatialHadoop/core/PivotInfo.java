/***********************************************************************
 * Copyright (c) 2015 by Regents of the University of Minnesota.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 *
 *************************************************************************/
package edu.umn.cs.spatialHadoop.core;

import edu.umn.cs.spatialHadoop.io.TextSerializerHelper;
import org.apache.hadoop.io.Text;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * Information about a specific cell in a grid.
 * Note: Whenever you change the instance variables that need to
 * be stored in disk, you have to manually fix the implementation of class
 * BlockListAsLongs
 * @author aseldawy
 *
 */
public class PivotInfo extends Point {

  /**
   * A unique ID for this cell in a file. This must be set initially when
   * cells for a file are created. It cannot be guessed from cell dimensions.
   */
  public int cellId;
  public double maxDis = -Double.MAX_VALUE;
  public double minDis = Double.MAX_VALUE;

  /**
   * Loads a cell serialized to the given stream
   * @param in
   * @throws IOException
   */
  public PivotInfo(DataInput in) throws IOException {
    this.readFields(in);
  }

  public PivotInfo(String in) {
    this.fromText(new Text(in));
  }

  public PivotInfo() {
    super();
  }

  public PivotInfo(int id, double x, double y) {
    super(x, y);
    this.cellId = id;
  }

  public PivotInfo(int id, double x, double y, double minDis, double maxDis) {
    super(x, y);
    this.cellId = id;
    this.minDis = minDis;
    this.maxDis = maxDis;
  }

  public PivotInfo(int id, Point pivotInfo) {
    this(id, pivotInfo.x, pivotInfo.y);
  }

  public PivotInfo(PivotInfo c) {
    this.set(c);
  }

  public void set(PivotInfo c) {
    if (c == null) {
      this.cellId = -1; // Invalid number
    } else {
      this.set(c.x,c.y);
      this.cellId = c.cellId; // Set cellId
      this.maxDis = c.maxDis;
      this.minDis = c.minDis;
    }
  }

  @Override
  public String toString() {
    return "Pivote #"+cellId+" "+super.toString() + " "+ minDis + " " +maxDis;
  }

  @Override
  public PivotInfo clone() {
    return new PivotInfo(cellId, x ,y, minDis, maxDis);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null)
      return false;
    return ((PivotInfo)obj).cellId == this.cellId;
  }

  @Override
  public int hashCode() {
    return (int) this.cellId;
  }

  @Override
  public int compareTo(Shape s) {
    return (int) (this.cellId - ((PivotInfo)s).cellId);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(cellId);
    super.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.cellId = in.readInt();
    super.readFields(in);
  }

  @Override
  public Text toText(Text text) {
    TextSerializerHelper.serializeInt(cellId, text, ',');
    TextSerializerHelper.serializeDouble(minDis, text, ',');
    TextSerializerHelper.serializeDouble(maxDis, text, ',');
    return super.toText(text);
  }

  @Override
  public void fromText(Text text) {
    this.cellId = TextSerializerHelper.consumeInt(text, ',');
    this.minDis = TextSerializerHelper.consumeDouble(text, ',');
    this.maxDis = TextSerializerHelper.consumeDouble(text, ',');
    super.fromText(text);
  }

  @Override
  public String toWKT() {
    return cellId+"\t"+super.toWKT();
  }
}
