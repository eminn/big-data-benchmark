/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.benchmark.jet;

import com.hazelcast.core.Member;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

public class MemberUtil {

    public static final Comparator<Member> MEMBER_COMPARATOR = (Comparator<Member>) (left, right) -> left.getUuid().compareTo(right.getUuid());

    public static Member[] sortMembers(Set<Member> members) {
        TreeSet<Member> sorted = new TreeSet<>(MEMBER_COMPARATOR);
        sorted.addAll(members);
        return sorted.toArray(new Member[sorted.size()]);
    }

    public static int indexOfMember(Set<Member> members, Member member) {
        Member[] sorted = sortMembers(members);
        return Arrays.binarySearch(sorted, member, MEMBER_COMPARATOR);
    }
}
