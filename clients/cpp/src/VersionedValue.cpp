/* -*- C++ -*-; c-basic-offset: 4; indent-tabs-mode: nil */
/*
 * Implementation for VersionedValue class.
 * 
 * Copyright (c) 2009 Webroot Software, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

#include <string.h>
#include <voldemort/Version.h>
#include <voldemort/VersionedValue.h>
#include <boost/shared_ptr.hpp>

#include "VectorClock.h"

namespace Voldemort {

using namespace boost;
using namespace std;

class VersionedValueImpl {
public:
    VersionedValueImpl(const VersionedValueImpl& v);
    VersionedValueImpl(shared_ptr<const string>& val,
                       shared_ptr<Version>& vers);
    VersionedValueImpl();
    ~VersionedValueImpl();

    shared_ptr<const string> value;
    shared_ptr<Version> version;
};

VersionedValueImpl::VersionedValueImpl()
    : value(new string("")), version(new VectorClock()) {

}

VersionedValueImpl::VersionedValueImpl(const VersionedValueImpl& v)
    : value(v.value), version(v.version) {

}

VersionedValueImpl::VersionedValueImpl(shared_ptr<const string>& val,
                                       shared_ptr<Version>& vers) 
    : value(val), version(vers) {

}

VersionedValueImpl::~VersionedValueImpl() {

}

VersionedValue::VersionedValue() {
    pimpl_ = new VersionedValueImpl();
}

VersionedValue::VersionedValue(const string* value,
                               Version* version) {
    shared_ptr<const string> valuep(value ? value : new string(""));
    shared_ptr<Version> versionp(version ? version : new VectorClock());
    pimpl_ = new VersionedValueImpl(valuep, versionp);
}

VersionedValue::VersionedValue(const VersionedValue& v) {
    pimpl_ = new VersionedValueImpl(*v.pimpl_);
}

VersionedValue::~VersionedValue() {
    delete pimpl_;
}

VersionedValue& VersionedValue::operator=(VersionedValue const & r) {
    pimpl_->value = r.pimpl_->value;
    pimpl_->version = r.pimpl_->version;

    return *this;
}

const string* VersionedValue::getValue() const {
    return pimpl_->value.get();
}

Version* VersionedValue::getVersion() const {
    return pimpl_->version.get();
}

void VersionedValue::setValue(const string* val) {
    shared_ptr<const string> valuep(val);
    pimpl_->value = valuep;
}

void VersionedValue::setVersion(Version* version) {
    shared_ptr<Version> versionp(version);
    pimpl_->version = versionp;
}

} /* namespace Voldemort */
