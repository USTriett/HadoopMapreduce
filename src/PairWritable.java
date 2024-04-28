package com.lab2.partone;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PairWritable implements WritableComparable<PairWritable> {
    private Text _termid;
    private Text _docid;

    public PairWritable(){
        set(new Text(String.valueOf(0)), new Text(String.valueOf(0)));
    }
    public PairWritable(Text termid, Text docid) {
        set(termid, docid);
    }


    public Text getTermid() {
        return _termid;
    }

    public Text getdocid() {
        return _docid;
    }

    public void set(Text termid, Text docid) {
        this._termid = termid;
        this._docid = docid;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        _termid.readFields(in);
        _docid.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        _termid.write(out);
        _docid.write(out);
    }

    @Override
    public String toString() {
        return _termid + "," + _docid;
    }

    @Override
    public int compareTo(PairWritable tp) {
        int cmp = _termid.compareTo(tp._termid);

        if (cmp != 0) {
            return cmp;
        }

        return _docid.compareTo(tp._docid);
    }

    @Override
    public int hashCode(){
        return _termid.hashCode()*163 + _docid.hashCode();
    }

    @Override
    public boolean equals(Object o)
    {
        if(o instanceof PairWritable)
        {
            PairWritable tp = (PairWritable) o;
            return _termid.equals(tp._termid) && _docid.equals(tp._docid);
        }
        return false;
    }
}
