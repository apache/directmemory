package org.apache.directmemory.measures;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

public class In {
	private double measure;
	
	public In(double measure) {
		this.measure = measure;
	}
	
	public long seconds() {
		return seconds(measure);
	}
	
	public long minutes() {
		return minutes(measure);
	}
	
	public long hours() {
		return hours(measure);
	}
	
	public long days() {
		return days(measure);
	}
	
	public static long seconds(double seconds) {
		return (long)seconds * 1000;
	}
	public static long minutes(double minutes) {
		return (long)seconds(minutes * 60);
	}
	public static long hours(double hours) {
		return (long)minutes(hours * 60);
	}
	public static long days(double days) {
		return (long)hours(days * 24);
	}
	
	public static In just(double measure) {
		return new In(measure);
	}
	public static In exactly(double measure) {
		return new In(measure);
	}
	public static In only(double measure) {
		return new In(measure);
	}
}
