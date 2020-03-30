package edu.hanyang.submit;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.hanyang.indexer.Tokenizer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.tartarus.snowball.ext.PorterStemmer;

public class TinySETokenizer implements Tokenizer {

	private SimpleAnalyzer analyzer;
	private PorterStemmer stemmer;

	public void setup() {
		analyzer = new SimpleAnalyzer();
		stemmer = new PorterStemmer();
	}

	public List<String> split(String text) {
		List<String> splitList = new ArrayList<>();
		try {
			TokenStream stream = analyzer.tokenStream(null, new StringReader(text));
			stream.reset();
			CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);

			while(stream.incrementToken()){
				stemmer.setCurrent(term.toString());
				stemmer.stem();
				splitList.add(stemmer.getCurrent());
			}
			stream.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		return splitList;
	}

	public void clean() {
		analyzer.close();
	}

}