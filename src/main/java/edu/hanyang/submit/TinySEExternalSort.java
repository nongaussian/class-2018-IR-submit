package edu.hanyang.submit;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.PriorityQueue;

import javax.swing.tree.MutableTreeNode;

//import com.sun.javaws.IconUtil;
import org.apache.commons.lang3.tuple.MutableTriple;
import edu.hanyang.indexer.ExternalSort;
import edu.hanyang.utils.DiskIO;
import jdk.nashorn.internal.objects.annotations.Getter;

class TripleSort implements Comparator<MutableTriple> {
	@Override
	public int compare(MutableTriple t1, MutableTriple t2) {
		if ((int)t1.getLeft() > (int)t2.getLeft()) {
			return 1;
		} else if ((int)t1.getLeft() == (int)t2.getLeft()) {
			if ((int)t1.getMiddle() > (int)t2.getMiddle()) {
				return 1;
			} else if ((int)t1.getMiddle() == (int)t2.getMiddle()) {
				return Integer.compare((int) t1.getRight(), (int) t2.getRight());
			} else {
				return -1;
			}
		} else {
			return -1;
		}
	}
}

public class TinySEExternalSort implements ExternalSort {
	public void sort(String infile, String outfile, String tmpdir, int blocksize, int nblocks) throws IOException {
		/* 
		 * blocksize * nblocks = available memory size
		 * nblocks = M 
		 * */
		
		File dir = new File(tmpdir);
		if (!dir.exists()) {
			dir.mkdirs();
		}

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(infile)));
		DataOutputStream dout = null;
		ArrayList<MutableTriple> runs = new ArrayList<>();

		int nElement = (nblocks * blocksize) / 24;
		int tot_input_size = dis.available();
		int rest_nElement = tot_input_size % nElement;

		int run_num = 0;
		try {
			/* Full Run */
			int loop_size = (tot_input_size/24 - (tot_input_size/24%nElement))/nElement;

			for (int i = 0; i < loop_size; i++) {
				for (int j = 0; j < nblocks; j++) {
					runs.add(new MutableTriple<>(dis.readInt(), dis.readInt(), dis.readInt()));
				}
				runs.sort(new TripleSort());

				/* init runs to file */
				dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + File.separator + "run_0_" + run_num + ".data")));
				for (MutableTriple<Integer,Integer,Integer> t : runs) {
					dout.writeInt(t.getLeft());
					dout.writeInt(t.getMiddle());
					dout.writeInt(t.getRight());
				}
				runs.clear();
				run_num++;

				dout.close();
			}
			
			/* Rest Run */
			if(rest_nElement != 0) {
				for (int i = 0; i < rest_nElement; i++) {
					runs.add(new MutableTriple<>(dis.readInt(),dis.readInt(),dis.readInt()));
				}

				runs.sort(new TripleSort());
				dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + File.separator + "run_0_" + run_num + ".data")));
				for (MutableTriple<Integer,Integer,Integer> t : runs) {
					dout.writeInt(t.getLeft());
					dout.writeInt(t.getMiddle());
					dout.writeInt(t.getRight());
				}
				runs.clear();
				dout.close();
			}
			dis.close();
			
			System.out.println(run_num + " : runs are init!");
			
			
			/* TODO : Start M-1 way Merge */
			int prevStepIdx = 0; 
			while (true) {
				/* Last Step */
				if (run_num < nblocks) {
					ArrayList<DataInputStream> fileList = new ArrayList<DataInputStream>();
					
					for (int i = 0; i < run_num; i++) {
						DataInputStream run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + prevStepIdx + "_" + i + ".data")));
						fileList.add(run);
					}
					
					_mergeSort(fileList, prevStepIdx + 1, outfile);
					
					break;
				} else {
//					int tot_iter = run_num % (nblocks - 1) == 0 ? run_num / (nblocks - 1) : (run_num % (nblocks - 1)) + 1;
//					System.out.println("tot_iter : " + tot_iter);
//									
//					for (int iter_idx = 0; iter_idx < tot_iter; iter_idx++) {
//						for (int i = 0; i < nblocks - 1; i++) {
//
//						}
//
//					}
					
				}
			}
						
			System.out.println("Fin!");
		} catch (EOFException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void _mergeSort(ArrayList<DataInputStream> fileArr, int nowStepIdx, String outfile) throws IOException {
		
		PriorityQueue<DataManager> pq = new PriorityQueue<DataManager>(new DataCmp());
		
		/* init PQ */
		for (DataInputStream f : fileArr) {
			try {
				DataManager dm = new DataManager(f.readInt(), f.readInt(), f.readInt(), fileArr.indexOf(f));
				pq.add(dm);				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		/* Now Only Consider Final Step*/
		DataOutputStream dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(outfile)));
		
		
		/* Write Next Step File */
		while (!pq.isEmpty()) {
			try {
				DataManager dm = pq.poll();
				
				dout.writeInt(dm.tuple.getLeft());
				dout.writeInt(dm.tuple.getMiddle());
				dout.writeInt(dm.tuple.getRight());
				dout.flush();
				
				dm.setTuple(fileArr.get(dm.index).readInt(), fileArr.get(dm.index).readInt(), fileArr.get(dm.index).readInt());
				
				pq.add(dm);
				
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		dout.close();
	}
	
	private class DataManager {
		public MutableTriple<Integer, Integer, Integer> tuple = new MutableTriple<Integer, Integer, Integer>();
		int index = -1;
		
		public DataManager(int left, int middle, int right, int idx){
			this.tuple.setLeft(left);
			this.tuple.setMiddle(middle);
			this.tuple.setRight(right);
			this.index = idx;
		}
		
		public void setTuple(int left, int middle, int right) {
			this.tuple.setLeft(left);
			this.tuple.setMiddle(middle);
			this.tuple.setRight(right);
		}

	}
	
	private class DataCmp implements Comparator<DataManager>{
		@Override
		public int compare(DataManager o1, DataManager o2) {
			return o1.tuple.compareTo(o2.tuple);
		}
		
	}
	
	public static void main(String[] args) throws IOException {
		String now_path = System.getProperty("user.dir");
		
		String infile = now_path + "/src/test/resources/test.data";
		String outfile = "/tmp/res.data";
		String tmpdir = now_path + "/tmp";
		int blocksize = 1024;
		int nblocks = 160;

		TinySEExternalSort externalSort = new TinySEExternalSort();
		try {
			externalSort.sort(infile, outfile, tmpdir, blocksize, nblocks);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		/* run data check test */
//		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(now_path + "/tmp/run0.data")));
//
//		while(dis.available() > 0){
//			int a= dis.readInt();
//			int b= dis.readInt();
//			int c= dis.readInt();
//			System.out.println(a+" "+b+" "+c);
//		}
//		dis.close();
		
		
	}

}
