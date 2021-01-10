#pragma once

namespace phxpaxos
{

class BytesBuffer
{
public:
    BytesBuffer();
    ~BytesBuffer();

    char * GetPtr();

    int GetLen();

    void Ready(const int iBufferLen);

private:
    char * m_pcBuffer;
    int m_iLen;
};
    
}
