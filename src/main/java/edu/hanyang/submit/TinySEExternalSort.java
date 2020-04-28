package edu.hanyang.submit;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import com.sun.javaws.IconUtil;
import org.apache.commons.lang3.tuple.MutableTriple;
import edu.hanyang.indexer.ExternalSort;

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
		/* nblocks = M */

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

				// runs to file
				dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + "/run" + run_num + ".data")));
				for (MutableTriple<Integer,Integer,Integer> t : runs) {
					dout.writeInt(t.getLeft());
					dout.writeInt(t.getMiddle());
					dout.writeInt(t.getRight());
				}
				runs.clear();
				run_num++;

				dout.close();
			}
			/* Rest Run*/
			if(rest_nElement !=0) {
				for (int i = 0; i < rest_nElement; i++) {
					runs.add(new MutableTriple<>(dis.readInt(),dis.readInt(),dis.readInt()));
				}

				runs.sort(new TripleSort());
				dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + "/run" + run_num + ".data")));
				for (MutableTriple<Integer,Integer,Integer> t : runs) {
					dout.writeInt(t.getLeft());
					dout.writeInt(t.getMiddle());
					dout.writeInt(t.getRight());
				}
				runs.clear();
				dout.close();
			}
			dis.close();

		} catch (EOFException | FileNotFoundException eof) {
			eof.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		String now_path = System.getProperty("user.dir");
//
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



		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("/Users/tw/Desktop/TinySE-submit/tmp/run1.data")));
		int cnt = 0;
		while(dis.available()>0){
			int a= dis.readInt();
			int b= dis.readInt();
			int c= dis.readInt();
			System.out.println(a+" "+b+" "+c + "     "+cnt);
			cnt++;
		}
	}

}


//		System.out.println(dis.available());
//		System.out.println(nElement * nblocks);
//
//		System.out.println(dis.available() / (nblocks * nElement));
//		System.out.println(dis.available() % (nblocks * nElement));


//		// TripleSort test
//		runs.add(new Triple(5, 4, 1));
//		runs.add(new Triple(1, 1, 1));
//		runs.add(new Triple(2, 4, 1));
//		runs.add(new Triple(5, 2, 1));
//		runs.add(new Triple(5, 4, 3));
//		runs.add(new Triple(1, 5, 3));
//		runs.add(new Triple(2, 2, 1));

//		runs.sort(new TripleSort());
//
//		for (Triple t : runs) {
//			System.out.printf("%d %d %d\n",t.word_id, t.doc_id, t.position);
//		}