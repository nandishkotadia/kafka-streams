package com.nandish.code.click;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.springframework.stereotype.Component;

import com.nandish.code.kafka.stream.model.Click;
import com.nandish.code.kafka.stream.model.ClickImpRecord;
import com.nandish.code.kafka.stream.model.Impression;

@Component
public class ClickValueJoiner {

	public ValueJoiner<Click, Impression, ClickImpRecord> getClickImpJoiner() {
		ValueJoiner<Click, Impression, ClickImpRecord> clickImpJoiner = new ValueJoiner<Click, Impression, ClickImpRecord>() {
			@Override
			public ClickImpRecord apply(Click c, Impression i) {
				if (c == null || i == null) {
					return null;
				}
				ClickImpRecord ci = new ClickImpRecord();
				ci.setImpressionId(i.getImpressionId());
				ci.setAdId(i.getAdId());
				ci.setBid(i.getBid());
				ci.setClickId(c.getClickId());
				ci.setEventDate(c.getEventDate());
				
				return ci;
			}
		};
		return clickImpJoiner;
	}
	
}
