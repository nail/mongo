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

#include "mongo/db/ops/field_checker.h"

#include "mongo/base/error_codes.h"
#include "mongo/db/field_ref.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    using mongoutils::str::stream;

namespace fieldchecker {

    Status isUpdatable(const FieldRef& field) {
        const size_t numParts = field.numParts();

        if (numParts == 0) {
            return Status(ErrorCodes::EmptyFieldName,
                          "An empty update path is not valid.");
        }

        for (size_t i = 0; i != numParts; ++i) {
            const StringData part = field.getPart(i);

            if (part.empty()) {
                return Status(ErrorCodes::EmptyFieldName,
                              mongoutils::str::stream() << "The update path '"
                              << field.dottedField()
                              << "' contains an empty field, which is not allowed.");
            }
        }

        return Status::OK();
    }

    bool isPositional(const FieldRef& fieldRef, size_t* pos, size_t* count) {

        // 'count' is optional.
        size_t dummy;
        if (count == NULL) {
            count = &dummy;
        }

        *count = 0;
        size_t size = fieldRef.numParts();
        for (size_t i=0; i<size; i++) {
            StringData fieldPart = fieldRef.getPart(i);
            if ((fieldPart.size() == 1) && (fieldPart[0] == '$')) {
                if (*count == 0) *pos = i;
                (*count)++;
            }
        }
        return *count > 0;
    }

} // namespace fieldchecker
} // namespace mongo
