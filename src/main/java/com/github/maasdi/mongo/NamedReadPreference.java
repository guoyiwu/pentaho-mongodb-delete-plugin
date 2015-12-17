/**
 * Copyright (C) 2014 Maas Dianto (maas.dianto@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.maasdi.mongo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.mongodb.*;

public enum NamedReadPreference {

  PRIMARY(ReadPreference.primary()),
  PRIMARY_PREFERRED(ReadPreference.primaryPreferred()),
  SECONDARY(ReadPreference.secondary()),
  SECONDARY_PREFERRED(ReadPreference.secondaryPreferred()),
  NEAREST(ReadPreference.nearest());

  private ReadPreference pref = null;

  NamedReadPreference(ReadPreference pref){
    this.pref = pref;
  }

  public String getName(){
    return pref.getName();
  }

  public ReadPreference getPreference(){
    return pref;
  }

  public static Collection<String> getPreferenceNames(){
    ArrayList<String> prefs = new ArrayList<String>();

    for (NamedReadPreference preference: NamedReadPreference.values()){
      prefs.add(preference.getName());
    }

    return prefs;
  }

  public ReadPreference getTaggableReadPreference(DBObject firstTagSet, DBObject... remainingTagSets){

    List<TagSet> tagsList = toTagsList(firstTagSet, remainingTagSets);

    switch (this){
      case PRIMARY_PREFERRED:
        return ReadPreference.primaryPreferred(tagsList);
      case SECONDARY:
        return ReadPreference.secondary(tagsList);
      case SECONDARY_PREFERRED:
        return ReadPreference.secondaryPreferred(tagsList);
      case NEAREST:
        return ReadPreference.nearest(tagsList);
      default:
        return (pref instanceof TaggableReadPreference) ? pref : null;
    }
  }

  public static NamedReadPreference byName (String preferenceName){
    NamedReadPreference foundPreference = null;

    for (NamedReadPreference preference: NamedReadPreference.values()){
      if (preference.getName().equalsIgnoreCase(preferenceName)){
        foundPreference = preference;
        break;
      }
    }
    return foundPreference;
  }

  private static List<TagSet> toTagsList(DBObject firstTagSet, DBObject... remainingTagSets) {
    ArrayList tagsList = new ArrayList(remainingTagSets.length + 1);
    tagsList.add(toTags(firstTagSet));
    DBObject[] var3 = remainingTagSets;
    int var4 = remainingTagSets.length;

    for(int var5 = 0; var5 < var4; ++var5) {
      DBObject cur = var3[var5];
      tagsList.add(toTags(cur));
    }

    return tagsList;
  }

  private static TagSet toTags(DBObject tagsDocument) {
    ArrayList tagList = new ArrayList();
    Iterator var2 = tagsDocument.keySet().iterator();

    while(var2.hasNext()) {
      String key = (String)var2.next();
      tagList.add(new Tag(key, tagsDocument.get(key).toString()));
    }

    return new TagSet(tagList);
  }

}
