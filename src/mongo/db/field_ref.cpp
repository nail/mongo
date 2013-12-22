/**
 *    Copyright (C) 2012 10gen Inc.
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

#include "mongo/db/field_ref.h"

#include <algorithm> // for min

#include "mongo/util/log.h"
#include "mongo/util/assert_util.h"

namespace mongo {

    FieldRef::FieldRef() : _size(0) {}

    FieldRef::FieldRef(const StringData& path) : _size(0) {
        parse(path);
    }

    void FieldRef::parse(const StringData& path) {
        if (path.size() == 0) {
            return;
        }

        // We guarantee that accesses through getPart() will be valid while 'this' is. So we
        // keep a copy in a local sting.

        _dotted = path.toString();

        // Separate the field parts using '.' as a delimiter.
        char* beg = _fieldBase.get();
        char* cur = beg;
        char* end = beg + dottedField.size();
        while (true) {
            if (cur != end && *cur != '.') {
                cur++;
                continue;
            }

            appendPart(StringData(beg, cur - beg));

            if (cur != end) {
                *cur = '\0';
                beg = ++cur;
                continue;
            }

            break;
        }
    }

    void FieldRef::setPart(size_t i, const StringData& part) {
        dassert(i < _size);

        if (_replacements.size() != _size) {
            _replacements.resize(_size);
        }

        _replacements[i] = part.toString();
        if (i < kReserveAhead) {
            _fixed[i] = _replacements[i];
        }
        else {
            _variable[getIndex(i)] = _replacements[i];
        }
    }

    size_t FieldRef::appendPart(const StringData& part) {
        if (_size < kReserveAhead) {
            _fixed[_size] = part;
        }
        else {
            _variable.push_back(part);
        }
        return ++_size;
    }

    void FieldRef::reserialize() const {
        std::string nextDotted;
        // Reserve some space in the string. We know we will have, at minimum, a character for
        // each component we are writing, and a dot for each component, less one. We don't want
        // to reserve more, since we don't want to forfeit the SSO if it is applicable.
        nextDotted.reserve((_size * 2) - 1);

        // Concatenate the fields to a new string
        for (size_t i = 0; i != _size; ++i) {
            if (i > 0)
                nextDotted.append(1, '.');
            const StringData part = getPart(i);
            nextDotted.append(part.rawData(), part.size());
        }

        // Make the new string our contents
        _dotted.swap(nextDotted);

        // Fixup the parts to refer to the new string
        std::string::const_iterator where = _dotted.begin();
        for (size_t i = 0; i != _size; ++i) {
            StringData& part = (i < kReserveAhead) ? _fixed[i] : _variable[getIndex(i)];
            const size_t size = part.size();
            part = StringData(&*where, size);
            where += (size + 1); // account for '.'
        }

        // Drop any replacements
        _replacements.clear();
    }

    StringData FieldRef::getPart(size_t i) const {
        dassert(i < _size);

        if (i < kReserveAhead) {
            return _fixed[i];
        }
        else {
            return _variable[getIndex(i)];
        }
    }

    bool FieldRef::isPrefixOf( const FieldRef& other ) const {
        // Can't be a prefix if the size is equal to or larger.
        if ( _size >= other._size ) {
            return false;
        }

        // Empty FieldRef is not a prefix of anything.
        if ( _size == 0 ) {
            return false;
        }

        size_t common = commonPrefixSize( other );
        return common == _size && other._size > common;
    }

    size_t FieldRef::commonPrefixSize( const FieldRef& other ) const {
        if (_size == 0 || other._size == 0) {
            return 0;
        }

        size_t maxPrefixSize = std::min( _size-1, other._size-1 );
        size_t prefixSize = 0;

        while ( prefixSize <= maxPrefixSize ) {
            if ( getPart( prefixSize ) != other.getPart( prefixSize ) ) {
                break;
            }
            prefixSize++;
        }

        return prefixSize;
    }

    StringData FieldRef::dottedField( size_t offset ) const {
        if (_size == 0 || offset >= numParts() )
            return StringData();

        if (!_replacements.empty())
            reserialize();
        dassert(_replacements.empty());

        // Assume we want the whole thing
        StringData result(_dotted);

        // Strip off any leading parts we were asked to ignore
        for (size_t i = 0; i < offset; ++i) {
            const StringData part = getPart(i);
            result = StringData(
                result.rawData() + part.size() + 1,
                result.size() - part.size() - 1);
        }

        res.append(_fixed[0].rawData(), _fixed[0].size());
        for (size_t i=1; i<_size; i++) {
            res.append(1, '.');
            StringData part = getPart(i);
            res.append(part.rawData(), part.size());
        }
        return res;
    }

    bool FieldRef::equalsDottedField( const StringData& other ) const {
        StringData rest = other;

        for ( size_t i = 0; i < _size; i++ ) {

            StringData part = getPart( i );

            if ( !rest.startsWith( part ) )
                return false;

            if ( i == _size - 1 )
                return rest.size() == part.size();

            // make sure next thing is a dot
            if ( rest.size() == part.size() )
                return false;

            if ( rest[part.size()] != '.' )
                return false;

            rest = rest.substr( part.size() + 1 );
        }

        return false;
    }

    int FieldRef::compare(const FieldRef& other) const {
        const size_t toCompare = std::min(_size, other._size);
        for (size_t i = 0; i < toCompare; i++) {
            if (getPart(i) == other.getPart(i)) {
                continue;
            }
            return getPart(i) < other.getPart(i) ? -1 : 1;
        }

        const size_t rest = _size - toCompare;
        const size_t otherRest = other._size - toCompare;
        if ((rest == 0) && (otherRest == 0)) {
            return 0;
        }
        else if (rest < otherRest ) {
            return -1;
        }
        else {
            return 1;
        }
    }

    void FieldRef::clear() {
        _size = 0;
        _variable.clear();
        _fieldBase.reset();
        _replacements.clear();
    }

    std::ostream& operator<<(std::ostream& stream, const FieldRef& field) {
        return stream << field.dottedField();
    }

} // namespace mongo
