TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt

SOURCES += \
    SSTable.cpp \
    compaction.cpp \
    correctness.cc \
    kvstore.cc \
    memtable.cpp

HEADERS += \
    MurmurHash3.h \
    kvstore.h \
    kvstore_api.h \
    memtable.h \
    test.h \
    utils.h
