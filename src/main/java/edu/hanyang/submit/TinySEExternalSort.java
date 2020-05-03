package edu.hanyang.submit;

import java.io.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;
import org.apache.commons.lang3.tuple.MutableTriple;
import edu.hanyang.indexer.ExternalSort;
import org.omg.CORBA.INTERNAL;

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

		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(infile),blocksize));
		DataOutputStream dout = null;
		ArrayList<MutableTriple> runs = new ArrayList<>();

		// 하나의 Run에 들어가는 최대 튜플 갯수
		int nElement = (nblocks * blocksize) / 12;
		// 읽을 파일의 Byte 수
		int tot_input_size = dis.available();
		// 마지막 Run에 들어가는 튜플 갯수
		int rest_nElement = (tot_input_size/12) % nElement;

		int run_num = 0;
		try {
			/* Full Run */
			int loop_size = (tot_input_size/12 - rest_nElement)/nElement;
			for (int i = 0; i < loop_size; i++) {
				for (int j = 0; j < nElement; j++) {
					runs.add(new MutableTriple<>(dis.readInt(), dis.readInt(), dis.readInt()));
//					int a= dis.readInt();
//					int b= dis.readInt();
//					int c= dis.readInt();
//					System.out.println(a+" "+b+" "+c);
//					runs.add(new MutableTriple<>(a,b,c));
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
			System.out.println("Rest : "+rest_nElement);
			if(rest_nElement != 0) {
				for (int i = 0; i < rest_nElement; i++) {
					runs.add(new MutableTriple<>(dis.readInt(),dis.readInt(),dis.readInt()));
				}

				runs.sort(new TripleSort());
				dout = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tmpdir + File.separator + "run_0_" + run_num + ".data"),blocksize));
				for (MutableTriple<Integer,Integer,Integer> t : runs) {
					dout.writeInt(t.getLeft());
					dout.writeInt(t.getMiddle());
					dout.writeInt(t.getRight());
				}
				runs.clear();
				dout.close();
				run_num++;
			}
			dis.close();

			System.out.println(run_num + " : runs are init!");


			/* TODO : Start M-1 way Merge */
			int prevStepIdx = 0;
			while (true) {
				/* Last Step */
				if (run_num < nblocks) {
					ArrayList<DataInputStream> fileList = new ArrayList<>();

					System.out.println("### Final Merge ###");

					for (int i = 0; i < run_num; i++) {
						DataInputStream run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + prevStepIdx + "_" + i + ".data"), blocksize));
						System.out.println(File.separator + "run_" + prevStepIdx + "_" + i + ".data");
						fileList.add(run);
					}

					_mergeSort(fileList, prevStepIdx + 1, outfile);

					System.out.println("##################");

					break;
				} else {
					int tot_iter = 1;
					int tmp_run = run_num;
					while (tmp_run > nblocks) {
						tmp_run = run_num % (nblocks - 1) == 0 ? run_num / (nblocks - 1) : (run_num / (nblocks - 1)) + 1;
						tot_iter++;
					}
					System.out.println("nblocks: " + nblocks);
					System.out.println("run#,TOT_ITER: "+run_num+" "+tot_iter);
					ArrayList<DataInputStream> fileList = new ArrayList<>();

					for (int kIter = 1; kIter < tot_iter; kIter++) {
						/* Full */
						int full_cnt = run_num / nblocks;
						System.out.println("Full_cnt: "+full_cnt);
						for (int j = 0; j < full_cnt; j++) {
							System.out.println("### MERGE ###");

							for (int i = 0; i < nblocks; i++) {
								DataInputStream run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + (kIter - 1) + "_" + (j*nblocks + i) + ".data")));
								System.out.println(File.separator + "run_" + (kIter - 1) + "_" + (j*nblocks + i) + ".data");
								fileList.add(run);
							}
//							System.out.println("KIter : "+kIter);


							System.out.println("### ### ###");
							_mergeSort(fileList, kIter, tmpdir + File.separator + "run_" + kIter + "_" + j + ".data");

//							System.out.println(kIter+"END");

							fileList.clear();
						}

						/* Rest */
						if (run_num % nblocks > 0) {
							System.out.println("### MERGE ###");
							for (int i = 0; i < run_num % nblocks; i++) {
								DataInputStream run = new DataInputStream(new BufferedInputStream(new FileInputStream(tmpdir + File.separator + "run_" + (kIter - 1) + "_" + (full_cnt*nblocks + i) + ".data")));
								System.out.println(File.separator + "run_" + (kIter - 1) + "_" + (full_cnt*nblocks + i) + ".data");
								fileList.add(run);
							}
							System.out.println("### ### ###");
							_mergeSort(fileList, kIter, tmpdir + File.separator + "run_" + kIter + "_" + full_cnt + ".data");

							fileList.clear();
						}

						run_num = run_num % (nblocks-1) == 0 ? run_num / (nblocks-1) : (run_num / (nblocks-1)) + 1;
						if (run_num < nblocks){
							prevStepIdx = tot_iter - 1;
							continue;
						}

						System.out.println("RUN#: "+run_num);
					}

					fileList.clear();
				}
			}

			System.out.println("Fin!");
		} catch (EOFException | FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	private void _mergeSort(ArrayList<DataInputStream> fileArr, int nowStepIdx, String outfile) throws IOException {

		PriorityQueue<DataManager> pq = new PriorityQueue<>(new DataCmp());

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
//				System.out.println(dm.getTuple().getLeft()+ " " + dm.getTuple().getMiddle());

				dout.writeInt(dm.tuple.getLeft());
				dout.writeInt(dm.tuple.getMiddle());
				dout.writeInt(dm.tuple.getRight());
				dout.flush();

				if (fileArr.get(dm.index).available() > 0){
					dm.setTuple(fileArr.get(dm.index).readInt(), fileArr.get(dm.index).readInt(), fileArr.get(dm.index).readInt());
					pq.add(dm);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		dout.close();
	}

	private static class DataManager {
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

		public MutableTriple<Integer, Integer, Integer> getTuple() {
			return tuple;
		}

	}

	private class DataCmp implements Comparator<DataManager>{
		@Override
		public int compare(DataManager o1, DataManager o2) {
			MutableTriple<Integer, Integer, Integer> t1 = o1.getTuple();
			MutableTriple<Integer, Integer, Integer> t2 = o2.getTuple();
			if (t1.getLeft() > t2.getLeft()) {
				return 1;
			} else if (t1.getLeft().equals(t2.getLeft())) {
				if (t1.getMiddle() > t2.getMiddle()) {
					return 1;
				} else if (t1.getMiddle().equals(t2.getMiddle())) {
					return Integer.compare(t1.getRight(), t2.getRight());
				} else {
					return -1;
				}
			} else {
				return -1;
			}
		}
	}

	public static void main(String[] args) throws IOException {
//		String now_path = System.getProperty("user.dir");
//
//		String infile = now_path + "/src/test/resources/test.data";
//		String outfile = "/tmp/res.data";
//		String tmpdir = now_path + "/tmp";
//		int blocksize = 1024;
//		int nblocks = 160;
//
//		TinySEExternalSort externalSort = new TinySEExternalSort();
//		try {
//			externalSort.sort(infile, outfile, tmpdir, blocksize, nblocks);
//		} catch (Exception e) {
//			e.printStackTrace();
//		}




		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("./tmp/sorted.data")));
//		DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream("./tmp/run_0_73.data")));

		int cnt = 0;
		while(dis.available() > 0){
			int a= dis.readInt();
			int b= dis.readInt();
			int c= dis.readInt();
			System.out.println(a+" "+b+" "+c);
			cnt++;
		}
		System.out.println(cnt);

//		int cnt2 = 0;
//		while (dis2.available() > 0 ){
//			int a2= dis2.readInt();
//			int b2= dis2.readInt();
//			int c2= dis2.readInt();
//			System.out.println(a2+" "+b2+" "+c2);
//			cnt2++;
//		}
//
//		System.out.println(cnt + " "+ cnt2);
		dis.close();
	}

}