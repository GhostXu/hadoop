package folwsum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FolwBean implements WritableComparable<FolwBean>{

	private int upFolw;
	private int downFolw;
	private int sumFlow;
	
	public FolwBean() {
		
	}

	public FolwBean(int upFolw, int downFolw) {
		super();
		this.upFolw = upFolw;
		this.downFolw = downFolw;
		this.sumFlow = upFolw + downFolw ;
	}
	
	public FolwBean(int upFolw, int downFolw,int sumFlow) {
		super();
		this.upFolw = upFolw;
		this.downFolw = downFolw;
		this.sumFlow = sumFlow;
	}
	
	public void set(int upFolw, int downFolw) {
		this.upFolw = upFolw;
		this.downFolw = downFolw;
		this.sumFlow = upFolw+downFolw;
	}
	
	public int getUpFolw() {
		return upFolw;
	}
	public void setUpFolw(int upFolw) {
		this.upFolw = upFolw;
	}
	public int getDownFolw() {
		return downFolw;
	}
	public void setDownFolw(int downFolw) {
		this.downFolw = downFolw;
	}
	public int getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(int sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		upFolw = input.readInt();
		downFolw = input.readInt();
		sumFlow = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(upFolw);
		output.writeInt(downFolw);
		output.writeInt(sumFlow);
	}

	@Override
	public String toString() {
		return "FolwBean [upFolw=" + upFolw + ", downFolw=" + downFolw + ", sumFlow="
				+ sumFlow + "]";
	}

	@Override
	public int compareTo(FolwBean o) {
		return this.sumFlow-o.getSumFlow()>0?-1:(this.sumFlow-o.getSumFlow()==0?0:1);
	}
}
