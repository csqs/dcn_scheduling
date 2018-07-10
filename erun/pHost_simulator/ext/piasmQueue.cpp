#include <limits.h>
#include <iostream>

#include "piasmQueue.h"
#include "dctcpPacket.h"

#include "../run/params.h"
#include "../ext/factory.h"

extern double get_current_time();
extern void add_to_event_queue(Event *ev);
extern DCExpParams params;

/* PFabric Queue */
PIASMQueue::PIASMQueue(uint32_t id, double rate, uint32_t limit_bytes, int location)
    : Queue(id, rate, limit_bytes, location) {
    for (uint32_t i = 0; i < 8; i++) {
        priority_queues.push_back(Factory::get_queue(i, rate, limit_bytes, DROPTAIL_QUEUE, 0, location));
        priority_queue_length.push_back(0);
    }
    for (uint32_t i = 0; i < 8; i++) {
        priority_queues[i]->limit_bytes = limit_bytes;
    }
}

uint32_t PIASMQueue::find_port(Packet *p){
    if(this->src->type == HOST){
        if(params.load_balancing == 2 && params.queue_type == 7){
            uint32_t p_flow_now_priority = p->flow->flow_priority;
            uint32_t hash_port = 0;
            if(p_flow_now_priority == 0){
                p_flow_now_priority++;
                hash_port = (p->src->id + p->dst->id + p->flow->id) % 4;
                return hash_port;
            }
            else if(p_flow_now_priority == 1){
                hash_port = (p->src->id + p->dst->id + p->flow->id) % 4;
                return hash_port;
            }
            else{
                p_flow_now_priority --;
                Queue* q_dst_queue = ((Switch *) this->dst)->queues[16];
                uint32_t queue_least_length = 0;
                queue_least_length = ((PIASMQueue*) q_dst_queue)->priority_queue_length[p_flow_now_priority];
                for(uint32_t i = 1; i < 4; i++){
                    q_dst_queue = ((Switch *) this->dst)->queues[16 + i];
                    if(queue_least_length < ((PIASMQueue*) q_dst_queue)->priority_queue_length[p_flow_now_priority]){
                        queue_least_length = ((PIASMQueue*) q_dst_queue)->priority_queue_length[p_flow_now_priority];
                        hash_port = i;
                    }
                }
            }
            return hash_port;
        }
    }
}

void PIASMQueue::enque(Packet *packet) {
    if(location != 0){
        p_arrivals += 1;
        b_arrivals += packet->size;
        uint32_t packet_priority = packet->pf_priority;
        if (packet_priority == 0) packet_priority = 0;
        else packet_priority --;
    //    std::cout << p_arrivals
    //    << " p_arrival " << packet_priority
    //    << " packet_priority " << priority_queues[packet_priority]->packets.size()
    //    << " packets.size() " << location
    //    << " location " << packet->flow->id << " flow->id \n";
        
        if (bytes_in_queue + packet->size <= limit_bytes) {
            packets.push_back(packet);
            bytes_in_queue += packet->size;
            if (packets.size() >= params.dctcp_mark_thresh) {
                ((DctcpPacket*) packet)->ecn = true;
            }
            
            if (packet_priority >= 0) {
                if (priority_queues[packet_priority]->bytes_in_queue + packet->size <= priority_queues[packet_priority]->limit_bytes) {
                    priority_queues[packet_priority]->packets.push_back(packet);
                    priority_queues[packet_priority]->bytes_in_queue += packet->size;
                    for(uint32_t i = 0; i <= packet_priority; i++)
                        priority_queue_length[i]++;
                }
                else{
                    pkt_drop++;
                    drop(packet);
                }
            }
            else {
                if (priority_queues[0]->bytes_in_queue + packet->size <= priority_queues[packet_priority]->limit_bytes) {
                    priority_queues[0]->packets.push_back(packet);
                    priority_queues[0]->bytes_in_queue += packet->size;
                    for(uint32_t i = 0; i <= packet_priority; i++)
                        priority_queue_length[i]++;
                }
                else{
                    pkt_drop++;
                    drop(packet);
                }
            }
        }
        else {
            pkt_drop++;
            drop(packet);
        }
    }
    else{
        p_arrivals += 1;
        b_arrivals += packet->size;
        if (bytes_in_queue + packet->size <= (limit_bytes * 8)) {
            packets.push_back(packet);
            bytes_in_queue += packet->size;
            
            if (packets.size() >= params.dctcp_mark_thresh) {
                ((DctcpPacket*) packet)->ecn = true;
            }
        }
        else {
            pkt_drop++;
            drop(packet);
        }
    }
}

Packet* PIASMQueue::deque() {
    if(location != 0){
        for (uint32_t i = 0; i < 8; i++) {
            if(priority_queues[i]->bytes_in_queue > 0){
                Packet* pkt = priority_queues[i]->packets.front();
                priority_queues[i]->packets.pop_front();
                priority_queues[i]->bytes_in_queue -= pkt->size;
                for(uint32_t j = 0; j <= i; j++)
                    priority_queue_length[j]--;
                
                pkt->total_queuing_delay += get_current_time() - pkt->last_enque_time;
                if(pkt->type ==  NORMAL_PACKET){
                    if(pkt->flow->first_byte_send_time < 0)
                        pkt->flow->first_byte_send_time = get_current_time();
                    if(this->location == 0)
                        pkt->flow->first_hop_departure++;
                    if(this->location == 3)
                        pkt->flow->last_hop_departure++;
                }
                p_departures += 1;
                bytes_in_queue -= pkt->size;
                b_departures += pkt->size;
                
                uint32_t best_index = 0;
                for (uint32_t j = 0; j < packets.size(); j++) {
                    Packet* curr_pkt = packets[j];
                    if (curr_pkt->flow->id == pkt->flow->id) {
                        best_index = j;
                        break;
                    }
                }
                packets.erase(packets.begin() + best_index);
                
                return pkt;
            }
        }
        return NULL;
    }
    else{
        if (bytes_in_queue > 0) {
            Packet *p = packets.front();
            packets.pop_front();
            bytes_in_queue -= p->size;
            p_departures += 1;
            b_departures += p->size;
            return p;
        }
        return NULL;
    }
}