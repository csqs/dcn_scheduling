#ifndef PIAS_QUEUE_H
#define PIAS_QUEUE_H

#include "../coresim/queue.h"
#include "../coresim/packet.h"

#define PIAS_QUEUE 6

class PIASQueue : public Queue {
    public:
        PIASQueue(uint32_t id, double rate, uint32_t limit_bytes, int location);
        void enque(Packet *packet);
        Packet *deque();
    
        std::vector<Queue *> priority_queues;
};

#endif
