/**
 *    Copyright (C) 2013 10gen Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "mongo/db/jsobj.h"
#include "mongo/bson/mutable/document.h"
#include "mongo/bson/mutable/element.h"

namespace mongo {

    // Extracts the value for 'pattern' for both 'lhs' and 'rhs' and return true if 'lhs' <
    // 'rhs'. We expect that both 'lhs' and 'rhs' be key patterns.
    struct PatternElementCmp {
        BSONObj sortPattern;
        bool useWholeValue;

        PatternElementCmp()
            : sortPattern(BSONObj())
            , useWholeValue(true)  {}

        PatternElementCmp(const BSONObj& pattern)
            : sortPattern(pattern)
            , useWholeValue(pattern.hasField("")){
        }

        bool operator()(const mutablebson::Element& lhs, const mutablebson::Element& rhs) const {
            if (useWholeValue) {
                const int comparedValue = lhs.compareWithElement( rhs, false );

                const bool reversed = (sortPattern.firstElement().number() < 0 );

                return (reversed ? comparedValue > 0 : comparedValue < 0);
            }
            else {
                //TODO: Push on to mutable in the future, and to support non-contiguous Elements.
                BSONObj lhsObj = lhs.getType() == Object ?
                                            lhs.getValueObject() :
                                            lhs.getValue().wrap("");
                BSONObj rhsObj = rhs.getType() == Object ?
                                            rhs.getValueObject() :
                                            rhs.getValue().wrap("");

                BSONObj lhsKey = lhsObj.extractFields(sortPattern, true);
                BSONObj rhsKey = rhsObj.extractFields(sortPattern, true);

                return lhsKey.woCompare(rhsKey, sortPattern) < 0;
            }
        }
    };

} // namespace mongo
