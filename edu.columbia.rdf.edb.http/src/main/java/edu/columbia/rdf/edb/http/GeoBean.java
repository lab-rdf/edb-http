package edu.columbia.rdf.edb.http;

import org.abh.common.bioinformatics.annotation.Type;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"id", "series_accession", "accession", "platform"})
public class GeoBean extends Type {

	private String mSeriesAccession;
	private String mPlatform;

	public GeoBean(int id,
			String seriesAccession,
			String accession,
			String platform) {
		super(id, accession);
		
		mSeriesAccession = seriesAccession;
		mPlatform = platform;
	}
	
	@Override
	@JsonIgnore
	public String getName() {
		return super.getName();
	}
	
	@JsonGetter("accession")
	public String getAccession() {
		return super.getName();
	}
	
	@JsonGetter("series_accession")
	public String getSeriesAccession() {
		return mSeriesAccession;
	}

	@JsonGetter("platform")
	public String getPlatform() {
		return mPlatform;
	}
}
