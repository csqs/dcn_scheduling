#ifndef PIASM_QUEUE_H
#define PIASM_QUEUE_H

#include "../coresim/queue.h"
#include "../coresim/packet.h"

#define PIASM_QUEUE 7

class PIASMQueue : public Queue {
    public:
        PIASMQueue(uint32_t id, double rate, uint32_t limit_bytes, int location);
        void enque(Packet *packet);
        Packet *deque();
    
        std::vector<Queue *> priority_queues;
    
        uint32_t find_port(Packet *p);
        std::vector<uint32_t> priority_queue_length;
};

#endif
