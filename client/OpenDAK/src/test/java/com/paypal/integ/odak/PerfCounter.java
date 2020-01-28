package com.paypal.integ.odak;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PerfCounter {
	public AtomicLong reqCount;
	public AtomicLong latencySum;
	public List<Long> latencies;
	
	public PerfCounter(){
		this.reqCount = new AtomicLong(0);
		this.latencySum = new AtomicLong(0);
		latencies = new ArrayList<>();
	}
	
	public long print99Percentile(){
		List<Long> recordedLatencies = new ArrayList<>();
		
		int emptySlots = 0;
		for(Long latency: latencies){
			if(latency == null){
				emptySlots++;	
			}else{
				recordedLatencies.add(latency);
			}
		}
		
		int percentIndex = (int)((99 * recordedLatencies.size()) / (100));
		Collections.sort(recordedLatencies);
		System.out.println("Total recorded latency samples - " + recordedLatencies.size());
		System.out.println("Empty slots in latencies list - " + emptySlots);
		System.out.println("99th Percentile index - " + percentIndex);
		System.out.println("99th Percentile latency - " + recordedLatencies.get(percentIndex));
		
		int percent999Index = (int)((99.9 * recordedLatencies.size()) / (100));
		System.out.println("99.9th Percentile index - " + percent999Index);
		System.out.println("99.9th Percentile latency - " + recordedLatencies.get(percent999Index));
		
		int percent9999Index = (int)((99.99 * recordedLatencies.size()) / (100));
		System.out.println("99.99th Percentile index - " + percent9999Index);
		System.out.println("99.99th Percentile latency - " + recordedLatencies.get(percent9999Index));
		

		int percent99999Index = (int)((99.999 * recordedLatencies.size()) / (100));
		System.out.println("99.999th Percentile index - " + percent99999Index);
		System.out.println("99.999th Percentile latency - " + recordedLatencies.get(percent99999Index));
		
		System.out.println("Max latency - " + recordedLatencies.get(recordedLatencies.size() - 1));
		return recordedLatencies.get(percentIndex);	
	}
}