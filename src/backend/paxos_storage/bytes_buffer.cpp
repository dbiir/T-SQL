#include "bytes_buffer.h"
#include <stdio.h>
#include <assert.h>

namespace phxpaxos
{

#define DEFAULT_BUFFER_LEN 1048576

BytesBuffer :: BytesBuffer()
    : m_pcBuffer(nullptr), m_iLen(DEFAULT_BUFFER_LEN)
{
    m_pcBuffer = new char[m_iLen];
    assert(m_pcBuffer != nullptr);
}
    
BytesBuffer :: ~BytesBuffer()
{
    delete []m_pcBuffer;
}

char * BytesBuffer :: GetPtr()
{
    return m_pcBuffer;
}

int BytesBuffer :: GetLen()
{
    return m_iLen;
}

void BytesBuffer :: Ready(const int iBufferLen)
{
    if (m_iLen < iBufferLen)
    {
        delete []m_pcBuffer;
        m_pcBuffer = nullptr;

        while (m_iLen < iBufferLen)
        {
            m_iLen *= 2; 
        }

        m_pcBuffer = new char[m_iLen];
        assert(m_pcBuffer != nullptr);
    }
}

}


