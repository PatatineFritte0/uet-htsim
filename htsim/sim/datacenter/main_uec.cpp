// -*- c-basic-offset: 4; indent-tabs-mode: nil -*-
//#include "config.h"
#include <cassert>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string.h>
#include <atomic>

#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <zmq.hpp> //apt install libzmq3-dev cppzmq-dev
#include <nlohmann/json.hpp> //apt install nlohmann-json3-dev
#include <sys/file.h>

#include <math.h>
#include <unistd.h>
#include "network.h"
#include "pipe.h"
#include "eventlist.h"
#include "logfile.h"
#include "uec_logger.h"
#include "clock.h"
#include "uec_base.h"
#include "uec.h"
#include "uec_mp.h"
#include "uec_pdcses.h"
#include "compositequeue.h"
#include "topology.h"
#include "connection_matrix.h"
#include "pciemodel.h"
#include "oversubscribed_cc.h"


#include "fat_tree_topology.h"
#include "fat_tree_switch.h"

#include <list>

// Simulation params

//#define PRINTPATHS 1

#include "main.h"

int DEFAULT_NODES = 128;
uint32_t DEFAULT_TRIMMING_QUEUESIZE_FACTOR = 1;
uint32_t DEFAULT_NONTRIMMING_QUEUESIZE_FACTOR = 5;
// #define DEFAULT_CWND 50

EventList eventlist;

static const char * help =
"\t[-help] [-o FNAME] [-debug] [-log STR] [-conn_reuse]\n"
"\t[-end UINT] [-nodes UINT] [-tiers UINT] [-planes UINT]\n"
"\t[-receiver_cc_only] [-sender_cc_only] [-qa_gate INT]\n"
"\t[-target_q_delay NUM] [-queue_size_bdp_factor INT]\n"
"\t[-sender_cc_algo STR] [-sender_cc] [-receiver_cc]\n"
"\t[-load_balancing_algo STR] [-queue_type STR]\n"
"\t[-host_queue_type STR] [-cwnd UINT] -tm FNAME\n"
"\t[-topo FNAME] [-q UINT] [-sack_threshold UINT]\n"
"\t[-oversubscribed_cc] [-Ai NUM] [-Md NUM] [-alpha NUM]\n"
"\t[-force_disable_oversubscribed_cc]\n"
"\t[-enable_accurate_base_rtt]\n"
"\t[-disable_base_rtt_update_on_nack] [-sleek] [-ecn UINT UINT]\n"
"\t[-disable_trim] [-trimsize UINT] [-logtime NUM]\n"
"\t[-logtime_us NUM] [-failed UINT] [-linkspeed NUM]\n"
"\t[-pcie NUM] [-seed UINT] [-mtu UINT] [-paths UINT]\n"
"\t[-hop_latency NUM] [-switch_latency NUM]\n"
"\t[-ar_sticky_delta NUM] [-ar_granularity STR]\n"
"\t[-ar_method STR] [-strat STR]\n"
"\n"
"Notes:\n"
" FNAME: File name\n"
" STR:   String\n"
" INT:   Signed integer\n"
" UINT:  Unsigned integer\n"
" NUM:   Decimal number, e.g. 3.14, 0.03, 42, ...\n"
"\n"
"Options:\n"
"  -help                 Show this help message and exit\n"
"  -o FNAME              Log file. Default: logout.dat\n"
"  -debug                Debug mode. Default: False\n"
"  -log STR              Items to log. Can be specified multiple times.\n"
"                        Choices: flow_events, sink, nic, tor_downqueue,\n"
"                                 tor_upqueue, switch, traffic, queue_usage.\n"
"                        Default: flow_events\n"
"  -conn_reuse           Enable connection reuse. Default: False\n"
"  -end UINT             End time (us). Default: 1000\n"
"  -nodes UINT           Number of nodes. Default: 0\n"
"  -tiers UINT           Number of tiers. Choices: 2, 3. Default: 3\n"
"  -planes UINT          Number of planes/ports per NIC.\n"
"                        Choices: 1..8\n"
"                        Default: 1\n"
"  -sa                   Scale-Across Simulation\n"
"  -ds                   file descriptor of Datacenter in SA simulation\n"
"                        Must be used with -sa\n"
"  -id NUM               Specifies the datacenter identifier for Scale Across simulation\n"
"  -range NUM1 NUM2      Parameter for SA simulation that indicates the range of nodes for one Datacenter\n"
"                        Raccomanded in Scale Across simulation\n"
"  -receiver_cc_only     Enable receiver based Congestion Control Only.\n"
"                        Default: False\n"
"  -sender_cc_only       Enable sender based Congestion Control Only.\n"
"                        Default: False.\n"
"  -qa_gate INT          Set Quick Adapt to 2^INT. Negative values map to 2^3.\n"
"                        Default: -1 (i.e. 2^3)\n"
"  -target_q_delay NUM   Set target queueing delay (us). Default: Derived from\n"
"                        network parameters.\n"
"  -queue_size_bdp_factor UINT\n"
"                        Set queue size bandwidth-delay product factor.\n"
"                        Default: Derived from network parameters.\n"
"  -sender_cc_algo STR   Sender based Congestions Control algorithm.\n"
"                        Choices: dctcp, nscc, constant.\n"
"                        Default: nscc\n"
"  -sender_cc            Enable Sender based Congestion Control.\n"
"                        Default: True.\n"
"  -receiver_cc          Enable Receiver based Congestion Control.\n"
"                        Default: False.\n"
"  -load_balancing_algo STR\n"
"                        Load balancing algorithm.\n"
"                        Choices: bitmap, reps, reps_legacy, oblivious, mixed.\n"
"                        Default: mixed\n"
"  -queue_type STR       Queue type.\n"
"                        Choices: composite, composite_ecn, aeolus, aeolus_ecn.\n"
"                        Default: composite\n"
"  -host_queue_type STR  Host queue type.\n"
"                        Choices: swift, prio, fair_prio.\n"
"                        Default: fair_prio\n"
"  -cwnd UINT            Congestion window limit. Default: 'maxwnd' calculated\n"
"                        from network parameters.\n"
"  -tm FNAME             Traffic/Connection matrix input file.\n"
"                        Default: read from stdin\n"
"  -topo FNAME           Topology input file. Default: Auto-generated topo.\n"
"  -q UINT               Queue size (packets). Default: 0\n"
"  -sack_threshold UINT  Receiver Selective ACK threshold (bytes).\n"
"                        Default: 16384\n"
"  -oversubscribed_cc    Enable Receiver Oversubscribed Congestion Control.\n"
"                        Default: False\n"
"  -Ai NUM               Oversubscribed Congestion Control Additive Increase.\n"
"                        Default: 0.01\n"
"  -Md NUM               Oversubscribed Congestion Control Multiplicative Decrease.\n"
"                        Default: 0.5\n"
"  -alpha NUM            Oversubscribed Congestion Control Alpha.\n"
"                        Default: 0.5\n"
"  -force_disable_oversubscribed_cc\n"
"                        Disable Receiver Oversubscribed Congestion Control\n"
"                        even with OS topology. Default: False\n"
"  -enable_accurate_base_rtt\n"
"                        Enable accurate base Round Trip Time. Each flow uses\n"
"                        the accurate end-to-end delay for the current\n"
"                        sender/receiver pair as rtt upper bound.\n"
"                        Default: False\n"
"  -disable_base_rtt_update_on_nack\n"
"                        Disable using NACKs to update the base Round Trip Time.\n"
"                        Default: False\n"
"  -sleek                Enable SLEEK, the sender-based fast loss recovery heuristic.\n"
"                        Default: False\n"
"  -ecn UINT UINT        Configure Explicit Congestion Notification thresholds.\n"
"                        The two UINTs correspond to the Low and High thresholds\n"
"                        respectively.\n"
"                        Default: LO=0, HI=0\n"
"  -disable_trim         Disable trimming, dropping instead. Default: False\n"
"  -trimsize UINT        Trimmed packet size (bytes). Default: 64\n"
"  -logtime NUM          Sample time for sinklogger (ms). Default: 0.25\n"
"  -logtime_us NUM       Sample time for sinklogger (us). Default: 250\n"
"  -failed UINT          Number of failed links (failed to 25% linkspeed).\n"
"                        Default: 0\n"
"  -linkspeed NUM        Linkspeed specified (Mbps). Default: 100000\n"
"  -pcie NUM             PCIe rate multiplier (rate = linkspeed * NUM).\n"
"                        Default: 1.1\n"
"  -seed UINT            Random seed. Default: 13\n"
"  -mtu UINT             Maximum Transmission Unit, i.e. packet size (bytes).\n"
"                        Default: 4150\n"
"  -paths UINT           Number of paths. Default: 64\n"
"  -hop_latency NUM      Per hop wire latency set (us). Default: 1\n"
"  -switch_latency NUM   Switching latency (us). Default: 0\n"
"  -ar_sticky_delta NUM  Adaptive Routing sticky delta (us). Default: 10\n"
"  -ar_granularity STR   Adaptive Routing granularity.\n"
"                        Choices: packet, flow.\n"
"                        Default: packet\n"
"  -ar_method STR        Adaptive Routing method.\n"
"                        Choices: pause, queue, bandwidth, pqb, pq, pb, qb.\n"
"                        Default: queue\n"
"  -strat STR            Route strategy.\n"
"                        Choices: ecmp_host, rr_ecmp, ecmp_host_ecn, ecmp_ar,\n"
"                                 reactive_ecn, ecmp_host_ar, ecmp_rr.\n"
"                        Default: ecmp_host\n";

void exit_error(char* progr) {
    cout << "HTSIM is a high-performance discrete event simulator used for network\n"
            "simulation.\n"
            "\n"
            "Usage: " << progr << '\n' << help << endl;
    exit(1);
}

simtime_picosec calculate_rtt(FatTreeTopologyCfg* t_cfg, linkspeed_bps host_linkspeed) { 
    /*
    Using the host linkspeed here is not very accurate, but hopefully good enough for this usecase.
    */
    simtime_picosec rtt = 2 * t_cfg->get_diameter_latency() 
                + (Packet::data_packet_size() * 8 / speedAsGbps(host_linkspeed) * t_cfg->get_diameter() * 1000) 
                + (UecBasePacket::get_ack_size() * 8 / speedAsGbps(host_linkspeed) * t_cfg->get_diameter() * 1000);
    
    return rtt;
};

uint32_t calculate_bdp_pkt(FatTreeTopologyCfg* t_cfg, linkspeed_bps host_linkspeed) {
    simtime_picosec rtt = calculate_rtt(t_cfg, host_linkspeed);
    uint32_t bdp_pkt = ceil((timeAsSec(rtt) * (host_linkspeed/8)) / (double)Packet::data_packet_size()); 

    return bdp_pkt;
}

//SA FUNCTION AND CLASS

//function or class AI generated
// Custom StreamBuffer che aggiunge il prefisso
class PrefixedBuffer : public std::streambuf {
private:
    std::streambuf* dest;
    std::string prefix;
    std::string line_buffer;

    static inline int lock_fd = -1;

    void acquire_lock() {
        if (lock_fd == -1) {
            lock_fd = open("/tmp/datacenter_sim_lock.lock", O_CREAT | O_RDWR, 0666);
        }
        flock(lock_fd, LOCK_EX); // Sincronizza i processi a livello di kernel
    }

    void release_lock() {
        if (lock_fd != -1) {
            flock(lock_fd, LOCK_UN);
        }
    }

protected:
    int_type overflow(int_type c) override {
        if (c == traits_type::eof()) {
            return traits_type::not_eof(c);
        }

        char ch = static_cast<char>(c);
        line_buffer += ch;

        if (ch == '\n') {
            flush_line();
        }

        return c;
    }

    int sync() override {
        if (!line_buffer.empty()) {
            flush_line();
        }
        return dest->pubsync();
    }

    void flush_line() {
        if (line_buffer.empty()) return;

        acquire_lock();

        std::string full_output;
        if (!prefix.empty()) {
            full_output += prefix + " ";
        }
        full_output += line_buffer;

        dest->sputn(full_output.data(), full_output.size());
        dest->pubsync();

        release_lock();
        line_buffer.clear();
    }

public:
    PrefixedBuffer(std::streambuf* dest_buf, const std::string& pref)
        : dest(dest_buf), prefix(pref) {}
};

class StreamPrefixer {
private:
    std::ostream& stream;
    std::streambuf* old_buf;
    PrefixedBuffer new_buf;
    bool had_unitbuf;

public:
    StreamPrefixer(std::ostream& os, const std::string& prefix)
        : stream(os), 
          old_buf(os.rdbuf()), 
          new_buf(old_buf, prefix),
          had_unitbuf((os.flags() & std::ios::unitbuf) != 0) {
        
        // Disattiva unitbuf per evitare che cerr flushi a ogni singolo operatore <<
        stream.unsetf(std::ios::unitbuf);
        stream.rdbuf(&new_buf); 
    }

    ~StreamPrefixer() {
        stream.flush();
        stream.rdbuf(old_buf);
        // Ripristina lo stato originale di unitbuf alla distruzione
        if (had_unitbuf) {
            stream.setf(std::ios::unitbuf);
        }
    }
};

//stop AI generated

#define TIMEOUT 10

std::atomic<bool> fatal_error_detected(false);
vector<pid_t> pids;

void handle_sigchld(int sig) {
    int status;
    pid_t pid;
    
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        bool exited_normally = WIFEXITED(status);
        int exit_code = exited_normally ? WEXITSTATUS(status) : -1;

        if (!exited_normally || exit_code != EXIT_SUCCESS) {
            const char* err = "[GUIDE_ERROR] CRITIC ERROR: Child died unexpectedly.\n";
            write(STDERR_FILENO, err, strlen(err));
            
            fatal_error_detected.store(true);
        }
    }
}

void kill_all_children() {
    cerr << "Killing all childs"<< endl;
    for (pid_t pid : pids) {
        kill(pid, SIGKILL); 
    }
}

void err_guide_routine(){
    kill_all_children();
    cerr << "Simulation interrupted with an error." << endl;

    exit(EXIT_FAILURE);
}

std::vector<std::string> split(const std::string& str, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::stringstream ss(str);
    
    while (std::getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    
    return tokens;
}

struct EventHeaderUpdater {
    int child_id;
    char message[16];
    simtime_picosec next_event_time;
};

struct EventTimeLimit{
    char message[16];
    simtime_picosec time_limit;
};

struct ChildEvent {
    int id;
    simtime_picosec next_event_time;

    bool operator<(const ChildEvent& other) const {
        if (next_event_time != other.next_event_time) {
            return next_event_time < other.next_event_time;
        }
        return id < other.id;
    }
};

struct Range{
    long int init;
    long int fin;
};

struct RangeMessage{
    int id;
    char message[16];
    Range range;
};

#pragma pack(push, 1)
struct SerializedUecPacket {
    simtime_picosec time_of_event;
    uint32_t magic;
    uint32_t type;
    uint16_t size;  
    uint32_t dst;
    uint32_t flow_id;  
    uint32_t pathid;  
    uint32_t flags;  
  
    union {  
        struct {  
            uint64_t epsn;  
            uint64_t pull_target;  
            uint32_t ptype;
            uint8_t  ar;  
            uint8_t  is_header;
            uint16_t full_size;
            int32_t  trim_hop;
            int32_t  trim_direction;  
        } data;   
  
        struct {  
            uint64_t pullno;
            uint8_t  rnr;
            uint8_t  slow_pull;
            uint16_t ev;
        } pull;  
  
        struct {  
            uint64_t ref_ack;  
            uint64_t acked_psn;  
            uint64_t cumulative_ack;  
            uint64_t sack_bitmap;  
            uint16_t ev;  
            uint8_t  ecn_echo;  
            uint8_t  rcv_wnd_pen;  
            uint64_t recvd_bytes;  
            uint32_t out_of_order;  
            uint8_t  is_rts;  
            uint8_t  probe_ack;  
            uint8_t  rtx_echo;  
        } ack;  

        struct {  
            uint64_t ref_epsn;  
            uint16_t ev;  
            uint8_t  ecn_echo;  
            uint64_t recvd_bytes;  
            uint64_t target_bytes;  
            uint8_t  last_hop;  
        } nack;  

        struct {  
            uint64_t epsn;  
            uint64_t pull_target;  
        } rts;  
    } u;
};
#pragma pack(pop)

SerializedUecPacket serializeUecPacket(const Packet& pkt) {  
    SerializedUecPacket s;  
    memset(&s, 0, sizeof(s));  

    // ---------- comune ----------  
    s.magic   = 0x55454350;  
    s.type    = pkt.type();  
    s.size    = pkt.size();  
    s.dst     = pkt.dst();  
    s.flow_id = pkt.flow_id();  
    s.flags   = pkt.flags();  

    switch (pkt.type()) {  

        case UECDATA: {  
            const UecDataPacket& dp = (const UecDataPacket&)pkt;  
            s.pathid                = (uint32_t)dp.path_id();  
            s.u.data.epsn           = dp.epsn();  
            s.u.data.pull_target    = dp.pull_target();  
            s.u.data.ptype          = (uint32_t)dp.packet_type();  
            s.u.data.ar             = dp.ar();  
            s.u.data.is_header      = dp.header_only();
            s.u.data.full_size      = dp.size();
            s.u.data.trim_hop       = dp.trim_hop();  
            s.u.data.trim_direction = (int32_t)dp.trim_direction();  
            break;  
        }  

        case UECPULL: {  
            const UecPullPacket& pp = (const UecPullPacket&)pkt;  
            s.pathid            = pp.pathid();
            s.u.pull.pullno     = pp.pullno();  
            s.u.pull.rnr        = pp.is_rnr();
            s.u.pull.slow_pull  = pp.is_slow_pull();
            s.u.pull.ev         = s.pathid;
            break;  
        }  

        case UECACK: {  
            const UecAckPacket& ap = (const UecAckPacket&)pkt;  
            s.u.ack.ref_ack        = ap.ref_ack();  
            s.u.ack.acked_psn      = ap.acked_psn();  
            s.u.ack.cumulative_ack = ap.cumulative_ack();  
            s.u.ack.sack_bitmap    = ap.bitmap();  
            s.u.ack.ev             = ap.ev();  
            s.u.ack.ecn_echo       = ap.ecn_echo();  
            s.u.ack.rcv_wnd_pen    = ap.rcv_wnd_pen();  
            s.u.ack.recvd_bytes    = ap.recvd_bytes();  
            s.u.ack.out_of_order   = ap.ooo();  
            s.u.ack.is_rts         = ap.is_rts();  
            s.u.ack.probe_ack      = ap.is_probe_ack();  
            s.u.ack.rtx_echo       = ap.rtx_echo();  
            break;  
        }  

        case UECNACK: {  
            const UecNackPacket& np = (const UecNackPacket&)pkt;  
            s.u.nack.ref_epsn     = np.ref_ack();  
            s.u.nack.ev           = np.ev();  
            s.u.nack.ecn_echo     = np.ecn_echo();  
            s.u.nack.recvd_bytes  = np.recvd_bytes();  
            s.u.nack.target_bytes = np.target_bytes();  
            s.u.nack.last_hop     = np.last_hop();  
            break;  
        }  

        case UECRTS: {  
            const UecRtsPacket& rp = (const UecRtsPacket&)pkt;
            s.pathid           = (uint32_t)rp.path_id(); 
            s.u.rts.epsn        = rp.epsn();  
            s.u.rts.pull_target = rp.pull_target();  
            break;  
        }  

        default:  
            break; 
    }  
    return s;  
}

Packet* deserializeUecPacket(const SerializedUecPacket& s,  
                            PacketFlow& flow, const Route& route) {  
    switch (s.type) {  

        case UECDATA: {  
            UecDataPacket* p = UecDataPacket::newpkt(  
                flow, route,  
                s.u.data.epsn,  
                s.u.data.full_size,
                (UecBasePacket::PacketType)s.u.data.ptype,  
                s.u.data.pull_target,  
                s.dst);  
            p->set_ar(s.u.data.ar);  
            if (s.pathid != UINT32_MAX) p->set_pathid(s.pathid);  
            p->set_flags(s.flags);  
            if (s.u.data.is_header)  
                p->strip_payload();
            return p;  
        }  
    
        case UECPULL: {  
            UecPullPacket* p = UecPullPacket::newpkt(  
                flow, &route,  
                s.u.pull.pullno,  
                (uint16_t)s.pathid,  
                s.dst);  
            p->set_flags(s.flags);  
            if (s.u.pull.slow_pull)  
                p->set_slow_pull(true);  
            return p;  
        }  

        case UECACK: { 
            UecAckPacket* p = UecAckPacket::newpkt(  
                flow, &route,  
                s.u.ack.cumulative_ack,  
                s.u.ack.ref_ack,  
                s.u.ack.acked_psn,  
                s.u.ack.ev,
                s.u.ack.ecn_echo,  
                s.u.ack.recvd_bytes,  
                s.u.ack.rcv_wnd_pen,  
                s.dst);  
            p->set_bitmap(s.u.ack.sack_bitmap);  
            p->set_ooo(s.u.ack.out_of_order);  
            p->set_is_rts(s.u.ack.is_rts);  
            p->set_probe_ack(s.u.ack.probe_ack);  
            p->set_rtx_echo(s.u.ack.rtx_echo);  
            p->set_flags(s.flags);  
            return p;  
        }  

        case UECNACK: {   
            UecNackPacket* p = UecNackPacket::newpkt(  
                flow, &route,  
                s.u.nack.ref_epsn,  
                s.u.nack.ev,  
                s.u.nack.recvd_bytes,  
                s.u.nack.target_bytes,  
                s.dst);  
            p->set_ecn_echo(s.u.nack.ecn_echo);  
            p->set_last_hop(s.u.nack.last_hop);  
            p->set_flags(s.flags);  
            return p;  
        }  
    
        case UECRTS: {    
            UecRtsPacket* p = UecRtsPacket::newpkt(  
                flow, &route,  
                s.u.rts.epsn,  
                s.u.rts.pull_target,  
                s.dst);  
            p->set_pathid(s.pathid);  
            p->set_flags(s.flags);  
            return p;  
        }  

        default:  
            exit(EXIT_FAILURE);  
    }  
}


bool asIntersection(const std::vector<RangeMessage>& rangeSet) {
    if (rangeSet.size() < 2) return false;

    std::vector<RangeMessage> sortedRanges = rangeSet;

    std::sort(sortedRanges.begin(), sortedRanges.end(), 
        [](const RangeMessage& a, const RangeMessage& b) {
            return a.range.init < b.range.init;
        }
    );

    for (size_t i = 1; i < sortedRanges.size(); ++i) {
        if (sortedRanges[i].range.init <= sortedRanges[i - 1].range.fin) {
            std::cout << sortedRanges[i].id << " and " << sortedRanges[i - 1].id << " overlap!" << std::endl;
            return true;
        }
    }

    //i prefer O(O(Nlog(N)) + O(N)) to O(N^2)

    return false;
}

int calcTotNodes(const std::vector<RangeMessage>& rangeSet){
    int ris = 0;
    auto i = rangeSet.begin();

    for (; i != rangeSet.end(); ++i) {
        if(strcmp(i->message, "RANGED") == 0){
            ris += i->range.fin - i->range.init + 1;
        }else if(strcmp(i->message, "NODES") == 0){
            ris += i->range.init;
        }else{ //YOUDO
            //nothing
        }
    }

    return ris;
}

bool loadDistribution(std::vector<RangeMessage> &rangeVector, int rangeMax){
    if(calcTotNodes(rangeVector) > rangeMax) return false;

    vector<RangeMessage> ranged;
    vector<RangeMessage> nodes;
    vector<RangeMessage> youDo;

    for(const auto range: rangeVector){
        if(strcmp(range.message, "YOUDO") == 0)
            youDo.push_back(range);
        else if(strcmp(range.message, "NODES") == 0)
            nodes.push_back(range);
        else if(strcmp(range.message, "RANGED") == 0)
            ranged.push_back(range);
        else
            return false;
    }
    
    if(asIntersection(ranged)){
        cerr<<"Overlapped ranges"<<endl;
        return false;
    }

    int nodesLeft = rangeMax;
    vector<Range> gaps;
    gaps.push_back({0, rangeMax - 1});

    //now we are going to manage the load of DC in specification order

    //ranged (we know the number of nodes and what nodes we need)
    for(auto &range: ranged){
        long int fin = range.range.fin, 
                 init = range.range.init;

        if(fin >= rangeMax || init < 0){
            cerr<<"Range DC"<<range.id<<" out of legal range"<<endl;
            return false;
        }
        
        nodesLeft -= fin - init + 1;
        // we can avoid the check because of initial check with calcTotNodes
        //if(nodesLeft < 0) return false;
        
        for (size_t i = 0; i < gaps.size(); ++i) {
            Range &gap = gaps[i];
            if(gap.init > fin || gap.fin < init) continue;

            if (gap.init == init && gap.fin == fin) {
                std::swap(gap, gaps.back());
                gaps.pop_back();
            }else if (gap.init == init && gap.fin > fin) {
                gap.init = fin + 1;
            }else if (gap.init < init && gap.fin == fin) {
                gap.fin = init - 1;
            }else if (gap.init < init && gap.fin > fin) {
                int tempFin = gap.fin;
                gap.fin = init - 1;
                gaps.push_back({fin + 1, tempFin});
            }else{
                cerr<<"i'm not suppose to be here lol"<<endl;
                return false;
            }

            break;
        }
    }

    
    //nodes (we know only the number of nodes ctrictly needed)
    for(auto &node: nodes){
        long int n = node.range.init;
        nodesLeft -= n;
        
        int bestIdx = -1;
        int minRemaining = rangeMax + 1;

        for (size_t i = 0; i < gaps.size(); ++i) {
            int gapNodes = gaps[i].fin - gaps[i].init + 1;
            if (gapNodes >= n) {
                int remaining = gapNodes - n;
                if (remaining < minRemaining) { //best fit
                    minRemaining = remaining;
                    bestIdx = i;
                    if (remaining == 0) break; // perfect fit!
                }
            }
        }

        if (bestIdx == -1) {
            cerr << "Nodes DC" << node.id << " without an available Range" << endl;
            return false;
        }

        Range &chosenGap = gaps[bestIdx];
        node.range = {chosenGap.init, chosenGap.init + n - 1};

        if (chosenGap.fin - chosenGap.init + 1 == n) {
            std::swap(gaps[bestIdx], gaps.back());
            gaps.pop_back();
        } else {
            chosenGap.init += n;
        }
    }

    if(youDo.size() > nodesLeft){
        cerr<<"there is no nodes left for the self handled datacenter"<<endl;
        return false;
    }

    //youdo

    //to do...
    if(youDo.size() > 0){
        cerr<<"YouDo is not currently handled."<<endl;
        return false;
    }

    //....[14]...[16]..[54]..[128]


    if(!gaps.empty()){
        cerr<<"There is some gaps left after load distribution"<<endl;
        cout<<"Remaining gaps: "<<endl;
        for(const auto gap: gaps){
            cout<<"["<<gap.init<<","<<gap.fin<<"], ";
        }
        cout<<endl;
        return false;
    }


    rangeVector.clear();
    rangeVector.insert(rangeVector.end(), ranged.begin(), ranged.end());
    rangeVector.insert(rangeVector.end(), nodes.begin(), nodes.end());
    rangeVector.insert(rangeVector.end(), youDo.begin(), youDo.end());

    return true;
}

int searchIdRange(const std::vector<RangeMessage> &rangeVector, int dst){
    for(const auto range: rangeVector){
        if(range.range.init <= dst && range.range.fin >= dst) return range.id;
    }

    return -1;
}

void updateOrInsertChildEvent(std::set<ChildEvent>& queue_order_childs, const EventHeaderUpdater& updater) {
    // O(N)
    auto it = std::find_if(queue_order_childs.begin(), queue_order_childs.end(), 
        [child_id = updater.child_id](const ChildEvent& e) {
            return e.id == child_id;
        });

    if (it == queue_order_childs.end()) {
        // O(log N)
        queue_order_childs.insert(ChildEvent{updater.child_id, updater.next_event_time});
    } else {
        if (updater.next_event_time < it->next_event_time) {
            auto node = queue_order_childs.extract(it);
            node.value().next_event_time = updater.next_event_time;
            queue_order_childs.insert(std::move(node));
        }
    }
}

void pkt_toString(Packet* pkt){
    cout << " pkt type " << pkt->type()
         << " dst " << pkt->dst()
         << " flow_id " << pkt->flow_id()
         << " size " << pkt->size();

    switch (pkt->type()) {  
        case UECDATA: {                                    // data or trimmed header  
            const UecDataPacket& dp = static_cast<const UecDataPacket&>(*pkt);  
            cout << " UECDATA epsn " << dp.epsn()  
                << " pkttype " << dp.packet_type()        // 0=PULL 1=SPEC 2=RTX 3=PROBE  
                << " ar " << dp.ar()  
                << " header_only " << pkt->header_only()   // true = trimmed  
                << " size " << pkt->size() << endl;  
            break;  
        }  
        case UECRTS: {                                     // request-to-send  
            const UecRtsPacket& rp = static_cast<const UecRtsPacket&>(*pkt);  
            cout << " UECRTS epsn " << rp.epsn()  
                << " pull_target " << rp.pull_target() << endl;  
            break;  
        }  
        case UECACK: {                                     // positive ACK 
            const UecAckPacket& ap = static_cast<const UecAckPacket&>(*pkt);  
            cout << " UECACK cum_ack " << ap.cumulative_ack()  
                << " ref_ack " << ap.ref_ack()  
                << " bitmap 0x" << hex << ap.bitmap() << dec  
                << " ecn " << ap.ecn_echo()  
                << " probe_ack " << ap.is_probe_ack() << endl;  
            break;  
        }  
        case UECNACK: {                                    // negative ACK (loss/trim)  
            const UecNackPacket& np = static_cast<const UecNackPacket&>(*pkt);  
            cout << " UECNACK ref_epsn " << np.ref_ack()  
                << " ev " << np.ev()  
                << " last_hop " << np.last_hop() << endl;  
            break;  
        }  
        case UECPULL: {                                    // credit receiver→sender  
            const UecPullPacket& pp = static_cast<const UecPullPacket&>(*pkt);  
            cout << " UECPULL pullno " << pp.pullno()  
                << " slow " << pp.is_slow_pull() << endl;  
            break;  
        }  
        default:  
            cout << " non-UEC type " << pkt->type() << endl;
            exit(EXIT_FAILURE);
    }
}

struct PendingEntry { Packet* pkt; uint32_t route_choice; };

class MyTapSwitch : public Switch{  
public:  
    MyTapSwitch(EventList& el, const string& name, zmq::socket_t* s, Range r, FatTreeTopology* tp, int id)  
    : Switch(el, name), _socket(s), _range(r), _tp(tp), _id(id){}
    
    void receivePacket(Packet& pkt) override {
        cout<<"[TAP] forwarding: ";
        
        cout << timeAsUs(eventlist().now()) << " TAP " << _name;
        pkt_toString(&pkt);

        SerializedUecPacket packet = serializeUecPacket(pkt);
        packet.time_of_event = eventlist().now();

        if (_tp->cfg().isInRange(packet.dst)){
            cerr<<"Packet not suppose to be in TAP switch";
            exit(EXIT_FAILURE);
        }

        //forwarding to wan

        EventHeaderUpdater info;
        info.child_id = _id;
        strcpy(info.message, "WAN");
        info.next_event_time = 0;

        _socket->send(zmq::buffer(&info, sizeof(EventHeaderUpdater)), zmq::send_flags::sndmore);

        _socket->send(zmq::buffer(&packet, sizeof(SerializedUecPacket)), zmq::send_flags::none);

        eventlist().stop();
        pkt.free();
    }

    void receivePacket(Packet& pkt, VirtualQueue* prev) override { 
        receivePacket(pkt); 
    }  

    void recvFromWan(SerializedUecPacket s) {
        uint32_t choice = freeBSDHash(s.flow_id, s.pathid) % _routesToTop.size();
        Route* route = _routesToTop[choice];
        
        PacketFlow* flow = getOrCreateFlow(s.flow_id);
        
        Packet* pkt = deserializeUecPacket(s, *flow, *route);
        pkt->set_route(route);
        
        cout<<"[TAP] I've recived: ";
        pkt_toString(pkt);
        cout<<endl;

        _pendingPkts.insert({s.time_of_event, (PendingEntry){pkt, choice}});

        eventlist().sourceIsPending(*this, s.time_of_event); //forwarding when 
    }

    void doNextEvent() override {
        simtime_picosec now = eventlist().now();
        auto it = _pendingPkts.find(now);
        while (it != _pendingPkts.end() && it->first == now) {
            PendingEntry& e = it->second;

            //cout << "[TAP] Forwarding tap->top: ";
            //pkt_toString(e.pkt);

            _routesToTop[e.route_choice]->at(0)->receivePacket(*e.pkt);
            it = _pendingPkts.erase(it);  
        }  
    }

    Route* getNextHop(Packet& pkt, BaseQueue* ingress) override { 
        return NULL; 
    }

    void addTopRoute(BaseQueue* q, Pipe* p) {    
        Route* r = new Route();    
        r->push_back(q);    
        r->push_back(p);    
        r->push_back(q->getRemoteEndpoint());
        _routesToTop.push_back(r);  
    }

    private:

    PacketFlow* getOrCreateFlow(flowid_t id) {  
        auto it = _flows.find(id);  
        if (it != _flows.end()) return it->second;  
        PacketFlow* f = new PacketFlow(NULL); // NULL logger, o un logger tuo  
        f->set_flowid(id);                    // id deve essere < 1'000'000'000  
        _flows[id] = f;  
        return f;  
    }


    int _id;
    FatTreeTopology *_tp;
    Range _range;
    zmq::socket_t *_socket;
    map<flowid_t, PacketFlow*> _flows;
    vector<Route*> _routesToTop;
    multimap<simtime_picosec, PendingEntry> _pendingPkts;
};

using json = nlohmann::json;

vector<vector<string>> parseDC(const string& filePath) {
    ifstream file(filePath);
    if (!file.is_open()) {
        cerr << "Error: Unable to open the DC file at path: " << filePath << endl;
        exit(EXIT_FAILURE);
    }

    // Global map to store single or multi-value global attributes (Key -> Vector of strings)
    unordered_map<string, vector<string>> global;
    
    vector<vector<string>> dc;

    try {
        json data;
        file >> data;

        for (auto& [key, value] : data.items()) {
            
            if (value.is_array()) {
                if (key == "-ds") {
                    for (const auto& obj : value) {
                        // Ensure each element inside "-ds" is a JSON object
                        if (!obj.is_object()) {
                            cerr << "Each element inside the '-ds' array must be a JSON object." << endl;
                            exit(EXIT_FAILURE);
                        }

                        vector<string> attributeRow;
                        for (const auto& [k, v] : obj.items()) {
                            // Reserved global attributes cannot be defined inside "-ds" objects
                            if (k == "-tm") {
                                cerr << "The attribute '-tm' inside '-ds' must be defined in the global section, not per-object." << endl;
                                exit(EXIT_FAILURE);
                            }

                            attributeRow.push_back(k);

                            // Object nesting is not allowed inside "-ds" properties
                            if (v.is_object()) {
                                cerr << "The value of attribute '" << k << "' inside '-ds' cannot be a nested object." << endl;
                                exit(EXIT_FAILURE);
                            }

                            if (v.is_array()) {
                                for (auto& item : v) {
                                    // Array elements inside "-ds" properties must be atomic (no nested arrays or objects)
                                    if (item.is_array() || item.is_object()) {
                                        cerr << "Elements inside the array for attribute '" << k << "' must be atomic values." << endl;
                                        exit(EXIT_FAILURE);
                                    }

                                    string itemStr = item.is_string() ? item.get<string>() : item.dump();
                                    attributeRow.push_back(itemStr);
                                }
                            } else {
                                string valStr = v.is_string() ? v.get<string>() : v.dump();
                                attributeRow.push_back(valStr);
                            }
                        }

                        dc.push_back(attributeRow);
                    }
                } else {
                    // Regular global array attribute
                    vector<string> multipleValues;
                    for (auto& item : value) {
                        string itemStr = item.is_string() ? item.get<string>() : item.dump();
                        multipleValues.push_back(itemStr);
                    }
                    
                    global[key] = multipleValues;
                }
            } else if (value.is_object()) {
                cerr << "Nested objects at the global level are not supported (Found in key: '" << key << "')." << endl;
                exit(EXIT_FAILURE);
            } else {
                // Regular global single-value attribute
                string valStr = value.is_string() ? value.get<string>() : value.dump();
                global[key] = {valStr};
            }
        }

    } catch (const json::parse_error& e) {
        cerr << "JSON Syntax Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    } catch (const std::exception& e) {
        cerr << "Exception encountered: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }

    bool tm = false;
    for (const auto& [key, vect] : global) {
        if(key.compare("-tm") == 0){
            if (!(*vect.begin()).empty()){
                tm =true;
                break;
            }
        }
    }

    if(!tm){
        cerr << "-tm must be specified on global args"<< endl;
        exit(EXIT_FAILURE);
    }

    // Merge global attributes into each row of dc, skipping if the key already exists
    for (auto& args : dc) {
        for (const auto& [key, vect] : global) {
            
            // Check if the key is already present inside this row (args)
            auto it = find(args.begin(), args.end(), key);
            if (it != args.end()) {
                continue; // Skip if the key is already inside dc (global has less priority)
            }

            // Insert global attribute values and the key at the beginning
            for (auto attr : vect) {
                args.insert(args.begin(), attr);
            }
            args.insert(args.begin(), key);
        }
    }

    return dc;
}

map<int, vector<int>> parseRoutes(const string& filePath, int max) {
    ifstream file(filePath);
    if (!file.is_open()) {
        cerr << "Error: Unable to open the route file at path: " << filePath << endl;
        exit(EXIT_FAILURE);
    }

    map<int, vector<int>> parsedRoutes;
    try {
        json data;
        file >> data;

        for (auto& [key, value] : data.items()) {
            string dcNumb = key;

            if (dcNumb.empty() || !all_of(dcNumb.begin(), dcNumb.end(), [](unsigned char c) { return isdigit(c); })) { //isn't a number?
                cerr << "Error: In route file, '" << dcNumb << "' is not a valid numeric ID." << endl;
                exit(EXIT_FAILURE);
            }

            int index = stoi(dcNumb);
            if (index < 0 || index >= max) {
                cerr << "Error: In route file, ID " << dcNumb << " must be between 0 and " << (max - 1) << "." << endl;
                exit(EXIT_FAILURE);
            }

            if (value.is_array()) {
                for (const auto& obj : value) {

                    string strConnection;
                    if (obj.is_number()) {
                        strConnection = to_string(obj.get<int>());
                    } else if (obj.is_string()) {
                        strConnection = obj.get<string>();
                    } else {
                        strConnection = obj.dump();
                    }

                    if (strConnection.empty() || !all_of(strConnection.begin(), strConnection.end(), [](unsigned char c) { return isdigit(c); })) {
                        cerr << "Error: In route file, node " << dcNumb << " has connection '" << strConnection << "' which is not a valid ID." << endl;
                        exit(EXIT_FAILURE);
                    }

                    int conn = stoi(strConnection);
                    if (conn < 0 || conn >= max) {
                        cerr << "Error: In route file, connection " << dcNumb << "->" << strConnection << " must be < " << max << endl;
                        exit(EXIT_FAILURE);
                    }

                    parsedRoutes[index].push_back(conn);
                }
            } else {
                cerr << "Error: In route file, value for key '" << dcNumb << "' must be an array." << endl;
                exit(EXIT_FAILURE);
            }
        }
    } catch (const json::parse_error& e) {
        cerr << "JSON Syntax Error: " << e.what() << endl;
        exit(EXIT_FAILURE);
    } catch (const std::exception& e) {
        cerr << "Exception encountered: " << e.what() << endl;
        exit(EXIT_FAILURE);
    }

    // Default behavior: if a dc has no routes specified, connect it to all other nodes (all-to-all)
    for (int i = 0; i < max; i++) {
        if (parsedRoutes[i].empty()) {
            for (int j = 0; j < max; j++) {
                if (i != j) {
                    parsedRoutes[i].push_back(j);
                }
            }
        }
    }

    return parsedRoutes;
}

string endpoint = "ipc:///tmp/datacenter_guide.ipc";

int main(int argc, char **argv) {
    int i = 1;
    int ppid = 0;
    int process = 0;

    bool sa = false;
    
    zmq::context_t context(1);
    zmq::socket_t socket(context, zmq::socket_type::dealer);
    zmq::message_t resp;
    //check if this is a Scale-Across simulation
    if(!strcmp(argv[i],"-sa")){
        i++;
        sa = true;

        if(!strcmp(argv[i],"-dc")){ //i'm the father (GUIDE)
            string dcPath = argv[++i];
            
            //getting dc info.
            StreamPrefixer p1(std::cout, "[GUIDE] ");
            StreamPrefixer p2(std::cerr, "[GUIDE_ERROR] ");

            auto dcArgs = parseDC(dcPath);

            ppid = getpid();
            int countDC = 0;

            for(auto &child : dcArgs){
                child.insert(child.begin(), to_string(ppid));
                child.insert(child.begin(), "-sa");
                child.insert(child.begin(), argv[0]);
                countDC++;
                cout<<"argv on child "<<countDC<<": ";
                for(auto args : child){
                    cout<<args<<" ";
                }
                cout<<endl;
            }

            string tm;
            for(auto &child : dcArgs){
                auto it = find(child.begin(), child.end(), "-tm");
                if (it != child.end()) {
                    auto val_it = std::next(it);
                    if (val_it != child.end()) {
                        tm = *val_it;
                    }
                }
                break;
            }

            string routeFile;
            while(i<argc){
                if(!strcmp(argv[i],"-route")){
                    routeFile = argv[++i];
                }
                i++;
            }

            map<int, vector<int>> dcConnection;
            if(routeFile.empty()){ //all are connected
                cout<<"apply all-to-all routing"<<endl;
                for (int i = 0; i < countDC; ++i) {
                    for (int j = 0; j < countDC; ++j) {
                        if (i != j) {
                            dcConnection[i].push_back(j);
                        }
                    }
                }
            }else{
                cout<<"tryng to parse the route file"<<endl;
                dcConnection = parseRoutes(routeFile, countDC);
            }
            
            for (int k = 0; k < countDC; ++k) {
                cout<<"Routes of "<<k<<": [";
                for (auto& conn : dcConnection[k]) {
                    cout<<conn<<",";
                }
                cout<<"]"<<endl;
            }

            //exit(EXIT_FAILURE);
            //creation of signal listener of the Childs
            struct sigaction sa;
            sa.sa_handler = handle_sigchld;
            sigemptyset(&sa.sa_mask);
            sa.sa_flags = SA_RESTART | SA_NOCLDSTOP;
            sigaction(SIGCHLD, &sa, NULL);

            //creation of the childs

            for (const auto& dc : dcArgs) {

                pid_t pid = fork();
                pids.push_back(pid);

                if (pid == 0) {
                    //child

                    std::vector<char*> args;

                    for (const auto& arg : dc) {
                        args.push_back(const_cast<char*>(arg.c_str()));
                    }

                    args.push_back(nullptr);

                    execv(args[0], args.data());

                    cerr<<"[CHILD] ERROR TO CREATE THE PROCESS. shutting down..."<<endl;
                    exit(EXIT_FAILURE);
                }

                if (pid < 0) {
                    cerr<<"[GUIDE] ERROR TO GENERATE A CHILD"<<endl;

                    err_guide_routine();
                }
            }


            //creation of the socket ROUTER
            zmq::context_t context(1);
            
            zmq::socket_t guide_socket(context, zmq::socket_type::router);
            guide_socket.bind(endpoint);
            cout << "Receiving at " << endpoint << endl;

            zmq::message_t routing_id; // Frame 0: Child ID (socket ZeroMQ)
            zmq::message_t payload;     // Frame 1: payload
            
            //connection with children

            string response;
            int childCount = 0;

            vector<zmq::message_t> sockets(countDC);
            vector<bool> usedIds(countDC, false);
            vector<zmq::message_t> pendingChildren;
            
            int timeOut = 0;
            while (childCount < countDC && !fatal_error_detected.load() && timeOut < TIMEOUT) {
                try{
                    zmq::pollitem_t items[] = { { guide_socket.handle(), 0, ZMQ_POLLIN, 0 } };

                    int rc = zmq::poll(items, 1, std::chrono::milliseconds(200));

                    if (rc == 0){
                        timeOut++;
                        continue;
                    }
                    timeOut = 0;

                    guide_socket.recv(routing_id, zmq::recv_flags::none);
                    guide_socket.recv(payload, zmq::recv_flags::none);

                    string str(static_cast<char*>(payload.data()), payload.size());
                    vector<string> data = split(str, ' ');
                    
                    //cout<<"Arrived: "<<str<<endl;
                    
                    if(data[0].compare("ID") != 0){
                        cerr<<"Unaspected data (ID), shutting down..."<<endl;
                        err_guide_routine();
                    }
                    int id = stoi(data[1]);
                    if(id != -1){
                        if (id < 0 || id >= countDC || usedIds[id]) {
                            cerr << "Invalid or duplicate ID requested: " << id<< endl;
                            err_guide_routine();
                        }

                        sockets[id].copy(routing_id);
                        usedIds[id] = true;
                    }else{
                        zmq::message_t temp;
                        temp.copy(routing_id);
                        pendingChildren.push_back(move(temp));
                    }

                    childCount++;
                }catch(const zmq::error_t& e){
                    if (e.num() == EINTR) {
                        // if some child died
                        if (fatal_error_detected.load()) break;
                        continue; // false allarm
                    }
                    // Actual ZMQ error
                    cerr << "ZMQ Error: " << e.what() << endl;
                    fatal_error_detected.store(true);
                    break;
                }
            }
            if(timeOut == TIMEOUT){
                cerr<<"Time out"<<endl;
                err_guide_routine();
            }

            if(fatal_error_detected.load()) err_guide_routine();

            childCount = 0;
            int penIdx = 0;
            for(int j = 0; j<countDC ;j++){
                if(!usedIds[j]){
                    sockets[j].copy(pendingChildren[penIdx++]);
                }

                response = "OK "+to_string(j);

                zmq::message_t route;
                route.copy(sockets[j]);
                guide_socket.send(route, zmq::send_flags::sndmore);
                guide_socket.send(zmq::buffer(response), zmq::send_flags::none);
            }

            cout<<"All child are memorized and with an identity"<<endl;

            cout<<"Waiting childs nodes for load balancing"<<endl;

            //syncronize the childs TEMP
            for (pid_t pid : pids) {
                int status;
                waitpid(pid, &status, WUNTRACED);
            }
            if(fatal_error_detected.load()) err_guide_routine();
            for (pid_t pid : pids) {
                kill(pid, SIGCONT);
            }
            
            vector<RangeMessage> childRanges;
            
            childCount = 0;
            while (childCount < countDC && !fatal_error_detected.load() && timeOut < TIMEOUT) {
                try{

                    zmq::pollitem_t items[] = { { guide_socket.handle(), 0, ZMQ_POLLIN, 0 } };

                    int rc = zmq::poll(items, 1, std::chrono::milliseconds(1000)); //1*TIMEOUT minutes for timeout

                    if (rc == 0){
                        timeOut++;
                        continue;
                    }
                    timeOut = 0;

                    guide_socket.recv(routing_id, zmq::recv_flags::none);
                    guide_socket.recv(payload, zmq::recv_flags::none);

                    if(payload.size() != sizeof(RangeMessage)){
                        cerr<<"Unaspected data (RangeMessage), shutting down..."<<endl;
                        err_guide_routine();
                    }

                    RangeMessage data;
                    memcpy(&data, payload.data(), sizeof(RangeMessage));
                    
                    if(sizeof(data.range) != sizeof(Range)){
                        cerr<<"Unaspected data (Range), shutting down..."<<endl;
                        err_guide_routine();
                    }

                    if(!(sockets[data.id] == routing_id)){
                        cerr<<"Unaspected sender, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    if(strcmp(data.message, "RANGED") == 0){
                        if(data.range.fin == -1){
                            cerr<<"Incoerence on Ranged message, shutting down..."<<endl;
                            err_guide_routine();
                        }
                    }else if(strcmp(data.message, "NODES") == 0){
                        if(data.range.fin != -1){
                            cerr<<"Incoerence on Nodes message, shutting down..."<<endl;
                            err_guide_routine();
                        }
                    }else if(strcmp(data.message, "YOUDO") == 0){
                        //nothing
                    }else{
                        cerr<<"Unaspected message, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    childRanges.push_back(data);
                    childCount++;
                }catch(const zmq::error_t& e){
                    if (e.num() == EINTR) {
                        // if some child died
                        if (fatal_error_detected.load()) break;
                        continue; // false allarm
                    }
                    // Actual ZMQ error
                    cerr << "ZMQ Error: " << e.what() << endl;
                    fatal_error_detected.store(true);
                    break;
                }
            }

            if(timeOut == TIMEOUT){
                cerr<<"Time out"<<endl;
                err_guide_routine();
            }

            if(fatal_error_detected.load()) err_guide_routine();

            for(auto it : childRanges){
                cout<<"DATACENTER "<<it.id<<": message "<<it.message<<" with range ["<< it.range.init <<", "<< it.range.fin << "]"<<endl;
            }

            cout<<"Information of nodes and range of all childs acquired... proceeding to do load distribution. "<<endl;

            //Get the tot number of nodes on tm file
            int totNodes;
            ifstream fileTm(tm);
            if(fileTm.is_open()){
                totNodes = ConnectionMatrix::getNumNode(fileTm);
                fileTm.close();
            }else{
                cerr<<"Failed to open file, shutting down..."<<endl;
                err_guide_routine();
            }

            if(totNodes%2 != 0){
                cerr<<"The number of nodes in tm file must be even"<<endl;
                err_guide_routine();
            }

            if (!loadDistribution(childRanges, totNodes)){
                cerr<<"Impossibile Load Distribution, shutting down";
                err_guide_routine();
            }

            for(const auto& chiRa: childRanges){
                zmq::message_t route;
                route.copy(sockets[chiRa.id]);
                guide_socket.send(route, zmq::send_flags::sndmore);
                guide_socket.send(zmq::buffer(&chiRa.range, sizeof(Range)), zmq::send_flags::none);
            }

            cout<<"Waiting every child initializing their enviroment"<<endl;

            //syncronize the childs TEMP
            for (pid_t pid : pids) {
                int status;
                waitpid(pid, &status, WUNTRACED);
            }
            if(fatal_error_detected.load()) err_guide_routine();
            for (pid_t pid : pids) {
                kill(pid, SIGCONT);
            }

            cout<<"INITIALIZE PRE-SIMULATION"<<endl;

            set<ChildEvent> queue_order_childs;

            //INIT
            childCount = 0;
            while (childCount < countDC && !fatal_error_detected.load() && timeOut < TIMEOUT) {
                try{

                    zmq::pollitem_t items[] = { { guide_socket.handle(), 0, ZMQ_POLLIN, 0 } };

                    int rc = zmq::poll(items, 1, std::chrono::milliseconds(60000)); //1*TIMEOUT minutes for timeout

                    if (rc == 0){
                        timeOut++;
                        continue;
                    }
                    timeOut = 0;

                    guide_socket.recv(routing_id, zmq::recv_flags::none);
                    guide_socket.recv(payload, zmq::recv_flags::none);

                    if(payload.size() != sizeof(EventHeaderUpdater)){
                        cerr<<"Unaspected data, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    EventHeaderUpdater data;
                    memcpy(&data, payload.data(), sizeof(EventHeaderUpdater));

                    if(!(sockets[data.child_id] == routing_id)){
                        cerr<<"Unaspected sender, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    if(strcmp(data.message, "INIT") != 0){
                        cerr<<"Unaspected message, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    queue_order_childs.insert({data.child_id, data.next_event_time});


                    childCount++;
                }catch(const zmq::error_t& e){
                    if (e.num() == EINTR) {
                        // if some child died
                        if (fatal_error_detected.load()) break;
                        continue; // false allarm
                    }
                    // Actual ZMQ error
                    cerr << "ZMQ Error: " << e.what() << endl;
                    fatal_error_detected.store(true);
                    break;
                }
            }

            
            for (const auto& evento : queue_order_childs) {
                std::cout << "ID: " << evento.id << " | Simtime: " << evento.next_event_time << " ps"<<endl;
            }

            cout<<"STARTING THE SIMULATION..."<<endl;
            
            ChildEvent next, now;
            now = *queue_order_childs.begin();
            queue_order_childs.erase(queue_order_childs.begin());

            EventTimeLimit info;

            while (!fatal_error_detected.load()) {
                if(queue_order_childs.empty()){
                    strcpy(info.message, "NOLIMIT");
                }else{
                    next = *queue_order_childs.begin();

                    strcpy(info.message, "GO");
                    info.time_limit = next.next_event_time;
                }
               
                zmq::message_t route;
                route.copy(sockets[now.id]);
                guide_socket.send(route, zmq::send_flags::sndmore);
                guide_socket.send(zmq::buffer(&info, sizeof(EventTimeLimit)), zmq::send_flags::none);
                
                int rc = 0;
                timeOut = 0;
                while (rc == 0 && timeOut < TIMEOUT) {
                    zmq::pollitem_t items[] = { { guide_socket.handle(), 0, ZMQ_POLLIN, 0 } };
                    rc = zmq::poll(items, 1, std::chrono::milliseconds(1000));
                    if (rc == 0) timeOut++;
                }
                
                if(timeOut >= TIMEOUT){
                    cerr<<"TIMEOUT on CHILD "<<now.id<<endl;
                    err_guide_routine();
                }
                

                guide_socket.recv(routing_id, zmq::recv_flags::none);
                guide_socket.recv(payload, zmq::recv_flags::none);

                if(routing_id != sockets[now.id]){
                    cerr<<"Unaspected sender, shutting down..."<<endl;
                    err_guide_routine();
                }

                if(payload.size() != sizeof(EventHeaderUpdater)){
                    cerr<<"Unaspected data, shutting down..."<<endl;
                    err_guide_routine();
                }

                EventHeaderUpdater data;
                memcpy(&data, payload.data(), sizeof(EventHeaderUpdater));

                if(strcmp(data.message, "WAN") == 0){

                    guide_socket.recv(payload, zmq::recv_flags::none);

                    if(routing_id != sockets[now.id]){
                        cerr<<"Unaspected sender, shutting down..."<<endl;
                        err_guide_routine();
                    }
                    
                    if(payload.size() != sizeof(SerializedUecPacket)){
                        cerr<<"Unaspected data, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    SerializedUecPacket pkt;
                    memcpy(&pkt, payload.data(), sizeof(SerializedUecPacket));

                    int dest = searchIdRange(childRanges, pkt.dst);

                    if(dest < 0){
                        cerr<<"Destination out of range, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    int nextHop = -1;
                    auto it = dcConnection[now.id].begin();
                    while(it != dcConnection[now.id].end()){
                        if(*it == dest){
                            nextHop = dest;
                        }
                        it++;
                    }

                    if(nextHop != dest){
                        cerr<<"There is no connection between "<<now.id<<" and "<<dest<<", shutting down..."<<endl;
                        err_guide_routine();
                    }


                    //waiting for the update (this can be optimized)
                    guide_socket.recv(routing_id, zmq::recv_flags::none);
                    guide_socket.recv(payload, zmq::recv_flags::none);

                    if(routing_id != sockets[now.id]){
                        cerr<<"Unaspected sender, shutting down..."<<endl;
                        err_guide_routine();
                    }
                    
                    if(payload.size() != sizeof(EventHeaderUpdater)){
                        cerr<<"Unaspected data, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    //data will be processed after WAN block
                    memcpy(&data, payload.data(), sizeof(EventHeaderUpdater));
                    //cout<<"Mi e arrivato l'update di "<<data.child_id<<" che ammonta a "<<data.next_event_time<<endl;

                    //forward the pkt

                    strcpy(info.message, "NEW");
                    info.time_limit = 0;

                    route.copy(sockets[dest]);
                    guide_socket.send(route, zmq::send_flags::sndmore);
                    guide_socket.send(zmq::buffer(&info, sizeof(EventTimeLimit)), zmq::send_flags::none);

                    route.copy(sockets[dest]);
                    guide_socket.send(route, zmq::send_flags::sndmore);
                    guide_socket.send(zmq::buffer(&pkt, sizeof(SerializedUecPacket)), zmq::send_flags::none);
                       
                    //updating the dest
                    guide_socket.recv(routing_id, zmq::recv_flags::none);
                    guide_socket.recv(payload, zmq::recv_flags::none);

                    if(routing_id != sockets[dest]){
                        cerr<<"Unaspected sender, shutting down..."<<endl;
                        err_guide_routine();
                    }
                    
                    if(payload.size() != sizeof(EventHeaderUpdater)){
                        cerr<<"Unaspected data, shutting down..."<<endl;
                        err_guide_routine();
                    }

                    EventHeaderUpdater update;
                    memcpy(&update, payload.data(), sizeof(EventHeaderUpdater));
                    
                    if(strcmp(update.message, "UPDATE") == 0){
                        //cout<<"Messaggio da "<<update.child_id<<": "<<update.message<<endl;
                        updateOrInsertChildEvent(queue_order_childs, update);
                    }else{//ignora per ora
                        //cerr<<"non ha senso lol"<<endl;
                        //err_guide_routine();
                    }

                }
                
                if(strcmp(data.message, "UPDATE") == 0){
                    //cout<<"Mi e' arrivato "<<timeAsUs(data.next_event_time)<<" da "<<data.child_id<<endl;
                    queue_order_childs.insert({data.child_id, data.next_event_time});
                }else if(strcmp(data.message, "DONE") == 0){
                    //cout<<"Mi e' arrivato DONE da "<<data.child_id<<endl;
                }else{
                    cerr<<"Unaspected message, shutting down..."<<endl;
                    err_guide_routine();
                }


                if(queue_order_childs.empty() && (strcmp(data.message, "DONE") == 0)) //stop simulating
                    break;

                now = *queue_order_childs.begin();
                queue_order_childs.erase(queue_order_childs.begin());
            }

            if(fatal_error_detected.load())err_guide_routine();

            for(auto& id : sockets){
                strcpy(info.message, "STOP");

                zmq::message_t route;
                route.copy(id);
                guide_socket.send(route, zmq::send_flags::sndmore);
                guide_socket.send(zmq::buffer(&info, sizeof(EventTimeLimit)), zmq::send_flags::none);
            }

            cout << "Simulation as ended, waiting all child to terminate..." << endl;

            for (pid_t pid : pids) {
                if (pid <= 0) continue;

                int status;
                pid_t result;

                do {
                    result = waitpid(pid, &status, 0);
                } while (result == -1 && errno == EINTR);
            }

            cout << "All children have terminated. Exiting." << endl;
            exit(EXIT_SUCCESS);

        }else{ //i'm the child
            int temp = getppid();
            ppid = atoi(argv[i++]);

            cout<<"[CHILD] ACTUAL_PPID: "<<temp<<", AXPECTED_PPID: "<<ppid<<endl;
            
            if(temp != ppid){
                cerr<<"I'm not a child of guide... shutting down";
                exit(EXIT_FAILURE);
            }

            //connection to father with socket REQ
            socket.connect(endpoint);

            cout<<"[CHILD] Connected on GUIDE at: " << endpoint << endl;

            //setup the 
            string send;

            send = "ID -1";
            int j = 1;
            while(j<argc){
                if(!strcmp(argv[j],"-id")){
                    process = atoi(argv[++j]);
                    send = "ID "+to_string(process);
                    break;
                }
                j++;
            }

            //cout<<"sended: "<<send<<endl;
            socket.send(zmq::buffer(send), zmq::send_flags::none);

            socket.recv(resp, zmq::recv_flags::none);

            vector<string> words = split(resp.to_string(), ' ');

            if(words[0].compare("OK")){
                cerr<<"[CHILD] Unaspected message, shutting down..."<<endl;
                exit(EXIT_FAILURE);
            }

            process = stoi(words[1]);
            cout<<"[CHILD "<<process<<"] Information aquired, i'm the child number: "<< process << endl;

            /*
            cout<<"my args are: ";
            for(int j = 0; j<argc; j++){
                cout<<argv[j]<<" ";
            }
            cout<<endl;
            */
        }
    }

    //exit(EXIT_SUCCESS);

    Range range;
    range.init = 0;
    range.fin = -1;
    
    std::string prefix_out = "";
    std::string prefix_err = "";

    if (sa) {
        prefix_out = "[DATACENTER " + std::to_string(process) + "]";
        prefix_err = "[DATACENTER_ERROR " + std::to_string(process) + "]";
    }

    // Attiviamo i due prefixer in modo indipendente ma sincronizzati dallo stesso lock globale
    StreamPrefixer p_out(std::cout, prefix_out);
    StreamPrefixer p_err(std::cerr, prefix_err);

    //Clock c(timeFromSec(5 / 100.), eventlist);
    bool param_queuesize_set = false;
    uint32_t queuesize_pkt = 0;
    linkspeed_bps linkspeed = speedFromMbps((double)HOST_NIC);
    int packet_size = 4150;
    uint32_t path_entropy_size = 64;
    uint32_t cwnd = 0, no_of_nodes = 0;
    uint32_t tiers = 3; // we support 2 and 3 tier fattrees
    uint32_t planes = 1;  // multi-plane topologies
    uint32_t ports = 1;  // ports per NIC
    bool disable_trim = false; // Disable trimming, drop instead
    uint16_t trimsize = 64; // size of a trimmed packet
    simtime_picosec logtime = timeFromMs(0.25); // ms;
    stringstream filename(ios_base::out);
    simtime_picosec hop_latency = timeFromUs((uint32_t)1);
    simtime_picosec switch_latency = timeFromUs((uint32_t)0);
    queue_type qt = COMPOSITE;

    enum LoadBalancing_Algo { BITMAP, REPS, REPS_LEGACY, OBLIVIOUS, MIXED};
    LoadBalancing_Algo load_balancing_algo = MIXED;

    bool log_sink = false;
    bool log_nic = false;
    bool log_flow_events = true;

    bool log_tor_downqueue = false;
    bool log_tor_upqueue = false;
    bool log_traffic = false;
    bool log_switches = false;
    bool log_queue_usage = false;
    const double ecn_thresh = 0.5; // default marking threshold for ECN load balancing
    simtime_picosec target_Qdelay = 0;

    bool param_ecn_set = false;
    bool ecn = true;
    uint32_t ecn_low = 0;
    uint32_t ecn_high = 0;
    uint32_t queue_size_bdp_factor = 0;
    uint32_t topo_num_failed = 0;

    bool receiver_driven = false;
    bool sender_driven = true;

    RouteStrategy route_strategy = NOT_SET;
    
    int seed = 13;
    double pcie_rate = 1.1;

    filename << "logout.dat";
    int end_time = 1000;//in microseconds
    bool force_disable_oversubscribed_cc = false;
    bool enable_accurate_base_rtt = false;

    //unsure how to set this. 
    queue_type snd_type = FAIR_PRIO;

    float ar_sticky_delta = 10;
    FatTreeSwitch::sticky_choices ar_sticky = FatTreeSwitch::PER_PACKET;

    char* tm_file = NULL;
    char* topo_file = NULL;
    int8_t qa_gate = -1;
    bool conn_reuse = false;

    while (i<argc) {
        if ((!strcmp(argv[i],"-sa")) || (!strcmp(argv[i],"-dc")) ){
            cerr<<"The Scale Across flags must be on the first input as follows: -sa -dc ./file"<<endl;
            exit(EXIT_FAILURE);
        }else if (!strcmp(argv[i],"-id")){
            if(!sa){
                cerr<<"id can be used only in a Scale Across simulation"<<endl;
                exit(EXIT_FAILURE);
            }
            i++;
        }else if (!strcmp(argv[i],"-range")) {
            if(sa){
                range.init = atoi(argv[++i]);
                range.fin  = atoi(argv[++i]);
                if(range.init > range.fin){
                    cerr<<"Invalid range input"<<endl;
                    exit(EXIT_FAILURE);
                }
                cout<<"Range setted to ["<< range.init << ","<< range.fin <<"]"<<endl;

            }else{
                cerr<<"Range parameter is for Scale across simulation only"<<endl;
                exit(EXIT_FAILURE);
            }
        }else if (!strcmp(argv[i],"-o")) {
            filename.str(std::string());
            filename << argv[i+1];
            i++;
        } else if (!strcmp(argv[i],"-conn_reuse")){
            conn_reuse = true;
            cout << "Enabling connection reuse" << endl;
        } else if (!strcmp(argv[i],"-end")) {
            end_time = atoi(argv[i+1]);
            cout << "endtime(us) "<< end_time << endl;
            i++;            
        } else if (!strcmp(argv[i],"-nodes")) {
            no_of_nodes = atoi(argv[i+1]);
            cout << "no_of_nodes "<<no_of_nodes << endl;
            i++;
        } else if (!strcmp(argv[i],"-tiers")) {
            tiers = atoi(argv[i+1]);
            cout << "tiers " << tiers << endl;
            assert(tiers == 2 || tiers == 3);
            i++;
        } else if (!strcmp(argv[i],"-planes")) {
            planes = atoi(argv[i+1]);
            ports = planes;
            cout << "planes " << planes << endl;
            cout << "ports per NIC " << ports << endl;
            assert(planes >= 1 && planes <= 8);
            i++;
        } else if (!strcmp(argv[i],"-receiver_cc_only")) {
            UecSrc::_sender_based_cc = false;
            UecSrc::_receiver_based_cc = true;
            UecSink::_oversubscribed_cc = false;
            sender_driven = false;
            receiver_driven = true;
            cout << "receiver based CC enabled ONLY" << endl;
//        } else if (!strcmp(argv[i],"-disable_fd")) {
//            disable_fair_decrease = true;
//            cout << "fair_decrease disabled" << endl;
        } else if (!strcmp(argv[i],"-sender_cc_only")) {
            UecSrc::_sender_based_cc = true;
            UecSrc::_receiver_based_cc = false;
            UecSink::_oversubscribed_cc = false;
            sender_driven = true;
            receiver_driven = false;
            cout << "sender based CC enabled ONLY" << endl;
        } else if (!strcmp(argv[i],"-qa_gate")) {
            qa_gate = atof(argv[i+1]);
            cout << "qa_gate 2^" << qa_gate << endl;
            i++;
        } else if (!strcmp(argv[i],"-target_q_delay")) {
            target_Qdelay = timeFromUs(atof(argv[i+1]));
            cout << "target_q_delay" << atof(argv[i+1]) << " us"<< endl;
            i++;
        } else if (!strcmp(argv[i],"-queue_size_bdp_factor")) {
            queue_size_bdp_factor = atoi(argv[i+1]);
            cout << "Setting queue size to "<< queue_size_bdp_factor << "x BDP." << endl;
            i++;
        } else if (!strcmp(argv[i],"-sender_cc_algo")) {
            UecSrc::_sender_based_cc = true;
            sender_driven = true;
            
            if (!strcmp(argv[i+1],"dctcp")) 
                UecSrc::_sender_cc_algo = UecSrc::DCTCP;
            else if (!strcmp(argv[i+1],"nscc")) 
                UecSrc::_sender_cc_algo = UecSrc::NSCC;
            else if (!strcmp(argv[i+1],"constant")) 
                UecSrc::_sender_cc_algo = UecSrc::CONSTANT;
            else {
                cout << "UNKNOWN CC ALGO " << argv[i+1] << endl;
                exit(1);
            }    
            cout << "sender based algo "<< argv[i+1] << endl;
            i++;
        } else if (!strcmp(argv[i],"-sender_cc")) {
            UecSrc::_sender_based_cc = true;
            UecSink::_oversubscribed_cc = false;
            sender_driven = true;
            cout << "sender based CC enabled " << endl;
        } else if (!strcmp(argv[i],"-receiver_cc")) {
            UecSrc::_receiver_based_cc = true;
            receiver_driven = true;
            cout << "receiver based CC enabled " << endl;
        }
        else if (!strcmp(argv[i],"-load_balancing_algo")){
            if (!strcmp(argv[i+1], "bitmap")) {
                load_balancing_algo = BITMAP;
            } 
            else if (!strcmp(argv[i+1], "reps")) {
                load_balancing_algo = REPS;
            }
            else if (!strcmp(argv[i+1], "reps_legacy")) {
                load_balancing_algo = REPS_LEGACY;
            }
            else if (!strcmp(argv[i+1], "oblivious")) {
                load_balancing_algo = OBLIVIOUS;
            }
            else if (!strcmp(argv[i+1], "mixed")) {
                load_balancing_algo = MIXED;
            }
            else {
                cout << "Unknown load balancing algorithm of type " << argv[i+1] << ", expecting bitmap, reps or reps2" << endl;
                exit_error(argv[0]);
            }
            cout << "Load balancing algorithm set to  "<< argv[i+1] << endl;
            i++;
        }
        else if (!strcmp(argv[i],"-queue_type")) {
            if (!strcmp(argv[i+1], "composite")) {
                qt = COMPOSITE;
            } 
            else if (!strcmp(argv[i+1], "composite_ecn")) {
                qt = COMPOSITE_ECN;
            }
            else if (!strcmp(argv[i+1], "aeolus")){
                qt = AEOLUS;
            }
            else if (!strcmp(argv[i+1], "aeolus_ecn")){
                qt = AEOLUS_ECN;
            }
            else {
                cout << "Unknown queue type " << argv[i+1] << endl;
                exit_error(argv[0]);
            }
            cout << "queue_type "<< qt << endl;
            i++;
        } else if (!strcmp(argv[i],"-debug")) {
            UecSrc::_debug = true;
            UecPdcSes::_debug = true;
        } else if (!strcmp(argv[i],"-host_queue_type")) {
            if (!strcmp(argv[i+1], "swift")) {
                snd_type = SWIFT_SCHEDULER;
            } 
            else if (!strcmp(argv[i+1], "prio")) {
                snd_type = PRIORITY;
            }
            else if (!strcmp(argv[i+1], "fair_prio")) {
                snd_type = FAIR_PRIO;
            }
            else {
                cout << "Unknown host queue type " << argv[i+1] << " expecting one of swift|prio|fair_prio" << endl;
                exit_error(argv[0]);
            }
            cout << "host queue_type "<< snd_type << endl;
            i++;
        } else if (!strcmp(argv[i],"-log")){
            if (!strcmp(argv[i+1], "flow_events")) {
                log_flow_events = true;
            } else if (!strcmp(argv[i+1], "sink")) {
                cout << "logging sinks\n";
                log_sink = true;
            } else if (!strcmp(argv[i+1], "nic")) {
                cout << "logging nics\n";
                log_nic = true;
            } else if (!strcmp(argv[i+1], "tor_downqueue")) {
                cout << "logging tor downqueues\n";
                log_tor_downqueue = true;
            } else if (!strcmp(argv[i+1], "tor_upqueue")) {
                cout << "logging tor upqueues\n";
                log_tor_upqueue = true;
            } else if (!strcmp(argv[i+1], "switch")) {
                cout << "logging total switch queues\n";
                log_switches = true;
            } else if (!strcmp(argv[i+1], "traffic")) {
                cout << "logging traffic\n";
                log_traffic = true;
            } else if (!strcmp(argv[i+1], "queue_usage")) {
                cout << "logging queue usage\n";
                log_queue_usage = true;
            } else {
                exit_error(argv[0]);
            }
            i++;
        } else if (!strcmp(argv[i],"-cwnd")) {
            cwnd = atoi(argv[i+1]);
            cout << "cwnd "<< cwnd << endl;
            i++;
        } else if (!strcmp(argv[i],"-tm")){
            tm_file = argv[i+1];
            cout << "traffic matrix input file: "<< tm_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-topo")){
            topo_file = argv[i+1];
            cout << "FatTree topology input file: "<< topo_file << endl;
            i++;
        } else if (!strcmp(argv[i],"-q")){
            param_queuesize_set = true;
            queuesize_pkt = atoi(argv[i+1]);
            cout << "Setting queuesize to " << queuesize_pkt << " packets " << endl;
            i++;
        }
        else if (!strcmp(argv[i],"-sack_threshold")){
            UecSink::_bytes_unacked_threshold = atoi(argv[i+1]);
            cout << "Setting receiver SACK bytes threshold to " << UecSink::_bytes_unacked_threshold  << " bytes " << endl;
            i++;            
        }
        else if (!strcmp(argv[i],"-oversubscribed_cc")){
            UecSink::_oversubscribed_cc = true;
            cout << "Using receiver oversubscribed CC " << endl;
        }
        else if (!strcmp(argv[i],"-Ai")){
            OversubscribedCC::_Ai = atof(argv[i+1]);
            cout << "Using Ai "  << OversubscribedCC::_Ai << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-Md")){
            OversubscribedCC::_Md = atof(argv[i+1]);
            cout << "Using Md "  << OversubscribedCC::_Md << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-alpha")){
            OversubscribedCC::_alpha = atof(argv[i+1]);
            cout << "Using Alpha "  << OversubscribedCC::_alpha << endl;
            i+=1;
        }
        else if (!strcmp(argv[i],"-force_disable_oversubscribed_cc")){
            UecSink::_oversubscribed_cc = false;
            force_disable_oversubscribed_cc = true;
            cout << "Disabling receiver oversubscribed CC even with OS topology" << endl;
        }
        else if (!strcmp(argv[i],"-enable_accurate_base_rtt")){
            enable_accurate_base_rtt = true;
            cout << "Enable accurate base rtt configuration, each flow uses the accurate end-to-end delay for the current sender/receiver pair as rtt upper bound." << endl;
        }
        else if (!strcmp(argv[i],"-disable_base_rtt_update_on_nack")){
            UecSrc::update_base_rtt_on_nack = false;
            cout << "Disables using NACKs to update the base RTT." << endl;
        }
        else if (!strcmp(argv[i],"-sleek")){
            UecSrc::_enable_sleek = true;
            cout << "Using SLEEK, the sender-based fast loss recovery heuristic " << endl;
        }
        else if (!strcmp(argv[i],"-ecn")){
            // fraction of queuesize, between 0 and 1
            param_ecn_set = true;
            ecn = true;
            ecn_low = atoi(argv[i+1]); 
            ecn_high = atoi(argv[i+2]);
            i+=2;
        } else if (!strcmp(argv[i],"-disable_trim")) {
            disable_trim = true;
            cout << "Trimming disabled, dropping instead." << endl;
        } else if (!strcmp(argv[i],"-trimsize")){
            // size of trimmed packet in bytes
            trimsize = atoi(argv[i+1]);
            cout << "trimmed packet size: " << trimsize << " bytes\n";
            i+=1;
        } else if (!strcmp(argv[i],"-logtime")){
            double log_ms = atof(argv[i+1]);            
            logtime = timeFromMs(log_ms);
            cout << "logtime "<< log_ms << " ms" << endl;
            i++;
        } else if (!strcmp(argv[i],"-logtime_us")){
            double log_us = atof(argv[i+1]);            
            logtime = timeFromUs(log_us);
            cout << "logtime "<< log_us << " us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-failed")){
            // number of failed links (failed to 25% linkspeed)
            topo_num_failed = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-linkspeed")){
            // linkspeed specified is in Mbps
            linkspeed = speedFromMbps(atof(argv[i+1]));
            i++;
        } else if (!strcmp(argv[i],"-seed")){
            seed = atoi(argv[i+1]);
            cout << "random seed "<< seed << endl;
            i++;
        } else if (!strcmp(argv[i],"-mtu")){
            packet_size = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-paths")){
            path_entropy_size = atoi(argv[i+1]);
            cout << "no of paths " << path_entropy_size << endl;
            i++;
        } else if (!strcmp(argv[i],"-hop_latency")){
            hop_latency = timeFromUs(atof(argv[i+1]));
            cout << "Hop latency set to " << timeAsUs(hop_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-pcie")){
            UecSink::_model_pcie = true;
            pcie_rate = atof(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-switch_latency")){
            switch_latency = timeFromUs(atof(argv[i+1]));
            cout << "Switch latency set to " << timeAsUs(switch_latency) << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_sticky_delta")){
            ar_sticky_delta = atof(argv[i+1]);
            cout << "Adaptive routing sticky delta " << ar_sticky_delta << "us" << endl;
            i++;
        } else if (!strcmp(argv[i],"-ar_granularity")){
            if (!strcmp(argv[i+1],"packet"))
                ar_sticky = FatTreeSwitch::PER_PACKET;
            else if (!strcmp(argv[i+1],"flow"))
                ar_sticky = FatTreeSwitch::PER_FLOWLET;
            else  {
                cout << "Expecting -ar_granularity packet|flow, found " << argv[i+1] << endl;
                exit(1);
            }   
            i++;
        } else if (!strcmp(argv[i],"-ar_method")){
            if (!strcmp(argv[i+1],"pause")){
                cout << "Adaptive routing based on pause state " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pause;
            }
            else if (!strcmp(argv[i+1],"queue")){
                cout << "Adaptive routing based on queue size " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_queuesize;
            }
            else if (!strcmp(argv[i+1],"bandwidth")){
                cout << "Adaptive routing based on bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_bandwidth;
            }
            else if (!strcmp(argv[i+1],"pqb")){
                cout << "Adaptive routing based on pause, queuesize and bandwidth utilization " << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pqb;
            }
            else if (!strcmp(argv[i+1],"pq")){
                cout << "Adaptive routing based on pause, queuesize" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pq;
            }
            else if (!strcmp(argv[i+1],"pb")){
                cout << "Adaptive routing based on pause, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_pb;
            }
            else if (!strcmp(argv[i+1],"qb")){
                cout << "Adaptive routing based on queuesize, bandwidth utilization" << endl;
                FatTreeSwitch::fn = &FatTreeSwitch::compare_qb; 
            }
            else {
                cout << "Unknown AR method expecting one of pause, queue, bandwidth, pqb, pq, pb, qb" << endl;
                exit(1);
            }
            i++;
        } else if (!strcmp(argv[i],"-strat")){
            if (!strcmp(argv[i+1], "ecmp_host")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
            } else if (!strcmp(argv[i+1], "rr_ecmp")) {
                //this is the host route strategy;
                route_strategy = ECMP_FIB_ECN;
                qt = COMPOSITE_ECN_LB;
                //this is the switch route strategy. 
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR_ECMP);
            } else if (!strcmp(argv[i+1], "ecmp_host_ecn")) {
                route_strategy = ECMP_FIB_ECN;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                qt = COMPOSITE_ECN_LB;
            } else if (!strcmp(argv[i+1], "reactive_ecn")) {
                // Jitu's suggestion for something really simple
                // One path at a time, but switch whenever we get a trim or ecn
                //this is the host route strategy;
                route_strategy = REACTIVE_ECN;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
                qt = COMPOSITE_ECN_LB;
            } else if (!strcmp(argv[i+1], "ecmp_ar")) {
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ADAPTIVE_ROUTING);
            } else if (!strcmp(argv[i+1], "ecmp_host_ar")) {
                route_strategy = ECMP_FIB;
                FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP_ADAPTIVE);
                //the stuff below obsolete
                //FatTreeSwitch::set_ar_fraction(atoi(argv[i+2]));
                //cout << "AR fraction: " << atoi(argv[i+2]) << endl;
                //i++;
            } else if (!strcmp(argv[i+1], "ecmp_rr")) {
                // switch round robin
                route_strategy = ECMP_FIB;
                path_entropy_size = 1;
                FatTreeSwitch::set_strategy(FatTreeSwitch::RR);
            }
            i++;
        } else if (!strcmp(argv[i],"-help")) {
            exit_error(argv[0]);
        } else {
            cerr << "Unknown parameter " << argv[i] << endl;
            //exit_error(argv[0]);
            exit(EXIT_FAILURE);
        }
        i++;
    }

    if (end_time > 0 && logtime >= timeFromUs((uint32_t)end_time)){
        cout << "Logtime set to endtime" << endl;
        logtime = timeFromUs((uint32_t)end_time) - 1;
    }

    assert(trimsize >= 64 && trimsize <= (uint32_t)packet_size);

    cout << "Packet size (MTU) is " << packet_size << endl;

    srand(seed);
    srandom(seed);
    cout << "Parsed args\n";
    Packet::set_packet_size(packet_size);


    UecSrc::_mtu = Packet::data_packet_size();
    UecSrc::_mss = UecSrc::_mtu - UecSrc::_hdr_size;

    if (route_strategy==NOT_SET){
        route_strategy = ECMP_FIB;
        FatTreeSwitch::set_strategy(FatTreeSwitch::ECMP);
    }

    /*
    UecSink::_oversubscribed_congestion_control = oversubscribed_congestion_control;
    */

    FatTreeSwitch::_ar_sticky = ar_sticky;
    FatTreeSwitch::_sticky_delta = timeFromUs(ar_sticky_delta);
    FatTreeSwitch::_ecn_threshold_fraction = ecn_thresh;
    FatTreeSwitch::_disable_trim = disable_trim;
    FatTreeSwitch::_trim_size = trimsize;

    eventlist.setEndtime(timeFromUs((uint32_t)end_time));

    switch (route_strategy) {
    case ECMP_FIB_ECN:
    case REACTIVE_ECN:
        if (qt != COMPOSITE_ECN_LB) {
            fprintf(stderr, "Route Strategy is ECMP ECN.  Must use an ECN queue\n");
            exit(1);
        }
        assert(ecn_thresh > 0 && ecn_thresh < 1);
        // no break, fall through
    case ECMP_FIB:
        if (path_entropy_size > 10000) {
            fprintf(stderr, "Route Strategy is ECMP.  Must specify path count using -paths\n");
            exit(1);
        }
        break;
    case NOT_SET:
        fprintf(stderr, "Route Strategy not set.  Use the -strat param.  \nValid values are perm, rand, pull, rg and single\n");
        exit(1);
    default:
        break;
    }

    // prepare the loggers

    cout << "Logging to " << filename.str() << endl;
    //Logfile 
    Logfile logfile(filename.str(), eventlist);

    cout << "Linkspeed set to " << linkspeed/1000000000 << "Gbps" << endl;
    logfile.setStartTime(timeFromSec(0));

    vector<unique_ptr<UecNIC>> nics;

    UecSinkLoggerSampling* sink_logger = NULL;
    if (log_sink) {
        sink_logger = new UecSinkLoggerSampling(logtime, eventlist);
        logfile.addLogger(*sink_logger);
    }
    NicLoggerSampling* nic_logger = NULL;
    if (log_nic) {
        nic_logger = new NicLoggerSampling(logtime, eventlist);
        logfile.addLogger(*nic_logger);
    }
    TrafficLoggerSimple* traffic_logger = NULL;
    if (log_traffic) {
        traffic_logger = new TrafficLoggerSimple();
        logfile.addLogger(*traffic_logger);
    }
    FlowEventLoggerSimple* event_logger = NULL;
    if (log_flow_events) {
        event_logger = new FlowEventLoggerSimple();
        logfile.addLogger(*event_logger);
    }

    //UecSrc::setMinRTO(50000); //increase RTO to avoid spurious retransmits
    UecSrc* uec_src;
    UecSink* uec_snk;

    //Route* routeout, *routein;

    QueueLoggerFactory *qlf = 0;
    if (log_tor_downqueue || log_tor_upqueue) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_SAMPLING, eventlist);
        qlf->set_sample_period(logtime);
    } else if (log_queue_usage) {
        qlf = new QueueLoggerFactory(&logfile, QueueLoggerFactory::LOGGER_EMPTY, eventlist);
        qlf->set_sample_period(logtime);
    }


    unique_ptr<FatTreeTopologyCfg> topo_cfg;
    auto conns = make_unique<ConnectionMatrix>(no_of_nodes);

    if(sa){
        //sync all child TEMP
        raise(SIGSTOP);

        //initial configuration of topography

        if(topo_file && no_of_nodes != 0){
            cerr<<"Only one input is allowed between -topo and -node"<<endl;
            exit(EXIT_FAILURE);
        }
        if(topo_file){
            topo_cfg = FatTreeTopologyCfg::load(topo_file, memFromPkt(queuesize_pkt), qt, snd_type);
            
            if (range.fin != -1){
                if(topo_cfg->no_of_nodes() != range.fin - range.init + 1){
                    cerr<<"range input and topo file are incompatibles for node numbers"<<endl;
                    exit(EXIT_FAILURE);
                }
            }

            no_of_nodes = topo_cfg->no_of_nodes();
        }else if(no_of_nodes != 0 || range.fin != -1){
            if(no_of_nodes == 0){
                no_of_nodes = range.fin - range.init + 1;
            }
            if (range.fin != -1){
                if(no_of_nodes != range.fin - range.init + 1){
                    no_of_nodes = range.fin - range.init + 1;
                    cout<<"The node input and range nodes are different, i'm overwriting the nodes with range nodes ("<< no_of_nodes <<")"<<endl;
                }
            }

            topo_cfg = make_unique<FatTreeTopologyCfg>(tiers, no_of_nodes, linkspeed, memFromPkt(queuesize_pkt),
                                                hop_latency, switch_latency, 
                                                qt, snd_type);
        }else{
            cout<<"I don't know my range and nodes"<<endl;
        }

        //getting node range
        RangeMessage msgRange;
        msgRange.id = process;
        msgRange.range = range;
        if(no_of_nodes != 0){
            if(range.fin != -1){
                strcpy(msgRange.message, "RANGED");
            }else{
                strcpy(msgRange.message, "NODES");
                msgRange.range.init = no_of_nodes;
            }
        }else{
            strcpy(msgRange.message, "YOUDO");
        }
        socket.send(zmq::buffer(&msgRange, sizeof(RangeMessage)), zmq::send_flags::none);

        socket.recv(resp, zmq::recv_flags::none);
        memcpy(&range, resp.data(), sizeof(Range));

        if(no_of_nodes == 0){
            no_of_nodes = range.fin - range.init + 1;
            topo_cfg = make_unique<FatTreeTopologyCfg>(tiers, no_of_nodes, linkspeed, memFromPkt(queuesize_pkt),
                                                hop_latency, switch_latency, 
                                                qt, snd_type);
        }

        cout<<"I manage "<< no_of_nodes << " nodes in range ["<< range.init <<", "<< range.fin <<"]"<<endl;

        topo_cfg->set_range(range.init, range.fin);

        if (tm_file){
            cout << "Loading connection matrix from  " << tm_file << endl;

            if (!conns->load(tm_file, range.init, range.fin)){
                cout << "Failed to load connection matrix " << tm_file << endl;
                exit(EXIT_FAILURE);
            }
        }
        else {
            cout << "tm file must be specified" << endl;        
            exit(EXIT_FAILURE);
        }
    

    }else{ //not a Scale across simulation
        if (tm_file){
            cout << "Loading connection matrix from  " << tm_file << endl;

            if (!conns->load(tm_file)){
                cout << "Failed to load connection matrix " << tm_file << endl;
                exit(EXIT_FAILURE);
            }
        }
        else {
            cout << "Loading connection matrix from standard input" << endl;        
            conns->load(cin);
        }

        if (conns->N != no_of_nodes && no_of_nodes != 0){
            cout << "Connection matrix number of nodes is " << conns->N << " while I am using " << no_of_nodes << endl;
            exit(EXIT_FAILURE);
        }

        no_of_nodes = conns->N;
    
        if (topo_file) {
            topo_cfg = FatTreeTopologyCfg::load(topo_file, memFromPkt(queuesize_pkt), qt, snd_type);

            if (topo_cfg->no_of_nodes() != no_of_nodes) {
                cerr << "Mismatch between connection matrix (" << no_of_nodes << " nodes) and topology ("
                        << topo_cfg->no_of_nodes() << " nodes)" << endl;
                exit(EXIT_FAILURE);
            }
        } else {
            topo_cfg = make_unique<FatTreeTopologyCfg>(tiers, no_of_nodes, linkspeed, memFromPkt(queuesize_pkt),
                                                    hop_latency, switch_latency, 
                                                    qt, snd_type);
        }
    }

    FatTreeSwitch::_nodeOffSet = conns->N;


    if (!param_queuesize_set) {
        cout << "Automatic queue sizing enabled ";        
        if (queue_size_bdp_factor==0) {
            if (disable_trim) {
                queue_size_bdp_factor = DEFAULT_NONTRIMMING_QUEUESIZE_FACTOR;
                cout << "non-trimming";
            } else {
                queue_size_bdp_factor = DEFAULT_TRIMMING_QUEUESIZE_FACTOR;
                cout << "trimming";
            }
        }
        cout << " queue-size-to-bdp-factor is " << queue_size_bdp_factor << "xBDP"
             << endl;
    }

    simtime_picosec network_max_unloaded_rtt = calculate_rtt(topo_cfg.get(), linkspeed);

    mem_b queuesize = 0;
    if (!param_queuesize_set) {
        uint32_t bdp_pkt = calculate_bdp_pkt(topo_cfg.get(), linkspeed);
        mem_b queuesize_pkt = bdp_pkt * queue_size_bdp_factor;
        queuesize = memFromPkt(queuesize_pkt);
    } else {
        queuesize = memFromPkt(queuesize_pkt);
    }
    topo_cfg->set_queue_sizes(queuesize);

    if (topo_num_failed > 0) {
        topo_cfg->set_failed_links(topo_num_failed);
    }

    if (topo_cfg->get_oversubscription_ratio() > 1 && !UecSrc::_sender_based_cc && !force_disable_oversubscribed_cc) {
        UecSink::_oversubscribed_cc = true;
        OversubscribedCC::setOversubscriptionRatio(topo_cfg->get_oversubscription_ratio());
        cout << "Using simple receiver oversubscribed CC. Oversubscription ratio is " << topo_cfg->get_oversubscription_ratio() << endl;
    } 

    //2 priority queues; 3 hops for incast
    UecSrc::_min_rto = timeFromUs(15 + queuesize * 6.0 * 8 * 1000000 / linkspeed);
    cout << "Setting min RTO to " << timeAsUs(UecSrc::_min_rto) << endl;

    if (ecn){
        uint32_t bdp_pkt = calculate_bdp_pkt(topo_cfg.get(), linkspeed);
        if (!param_ecn_set) {
            ecn_low = memFromPkt(ceil(bdp_pkt * 0.2));
            ecn_high = memFromPkt(ceil(bdp_pkt * 0.8));
        } else {
            ecn_low = memFromPkt(ecn_low);
            ecn_high = memFromPkt(ecn_high);
        }
        cout << "Setting ECN to parameters low " << ecn_low << " high " << ecn_high <<  " enable on tor downlink " << !receiver_driven << endl;
        topo_cfg->set_ecn_parameters(true, !receiver_driven, ecn_low, ecn_high);
        assert(ecn_low <= ecn_high);
        assert(ecn_high <= queuesize);
    }

    cout << *topo_cfg << endl;

    if(sa && planes > 1){
        cerr<<"Planes >1 not handled in a Scale Across simulation"<<endl;
        exit(EXIT_FAILURE);
    }

    vector<unique_ptr<FatTreeTopology>> topo;
    MyTapSwitch* tap;
    topo.resize(planes);
    for (uint32_t p = 0; p < planes; p++) {
        topo[p] = make_unique<FatTreeTopology>(topo_cfg.get(), qlf, &eventlist, nullptr);  
        if(sa){
            uint32_t n_top = (topo_cfg->get_tiers() == 3) ? topo_cfg->no_of_cores() : topo_cfg->getNAGG();
            tap = new MyTapSwitch(eventlist, "TapSwitch_" + ntoa(p), &socket, range, topo[p].get(), process);

            for (uint32_t c = 0; c < n_top; c++) {  
                Switch* top_sw = (topo_cfg->get_tiers() == 3)  
                    ? topo[p]->switches_c[c] : topo[p]->switches_up[c];  
            
                // top -> tap
                BaseQueue* q_to_tap = topo[p]->alloc_queue(NULL, linkspeed, topo_cfg->queue_down(CORE_TIER), DOWNLINK, CORE_TIER, false, false);
                Pipe* p_to_tap = new Pipe(hop_latency, eventlist);
                q_to_tap->setRemoteEndpoint(tap);

                top_sw->addPort(q_to_tap);
                ((FatTreeSwitch*)top_sw)->setTapRoute(q_to_tap, p_to_tap);
            
                // tap -> top                                                 //should be setted
                BaseQueue* q_from_tap = topo[p]->alloc_queue(NULL, linkspeed, memFromPkt(100), UPLINK, CORE_TIER, false, false);   
                Pipe* p_from_tap = new Pipe(hop_latency, eventlist);
                q_from_tap->setRemoteEndpoint(top_sw);
                
                tap->addPort(q_from_tap);
                tap->addTopRoute(q_from_tap, p_from_tap);
            }
        }

        if (log_switches) {
            topo[p]->add_switch_loggers(logfile, logtime);
        }
    }
    cout << "network_max_unloaded_rtt " << timeAsUs(network_max_unloaded_rtt) << endl;

    if (UecSink::_oversubscribed_cc){
        OversubscribedCC::_base_rtt = network_max_unloaded_rtt;
    }
    
    //handle link failures specified in the connection matrix.
    for (size_t c = 0; c < conns->failures.size(); c++){
        failure* crt = conns->failures.at(c);
        
        if(sa){
            if(crt->dc != process) continue;
        }
        
        cout << "Adding link failure switch type" << crt->switch_type << " Switch ID " << crt->switch_id << " link ID "  << crt->link_id << endl;
        // xxx we only support failures in plane 0 for now.
        topo[0]->add_failed_link(crt->switch_type,crt->switch_id,crt->link_id);
    }

    // Initialize congestion control algorithms
    if (receiver_driven) {
        // TBD
    }
    if (sender_driven) {
        // UecSrc::parameterScaleToTargetQ();
        bool trimming_enabled = !disable_trim;
        UecSrc::initNsccParams(network_max_unloaded_rtt, linkspeed, target_Qdelay, qa_gate, trimming_enabled);
    }

    vector<unique_ptr<UecPullPacer>> pacers;
    vector<PCIeModel*> pcie_models;
    vector<OversubscribedCC*> oversubscribed_ccs;

    for (size_t ix = 0; ix < no_of_nodes; ix++){
        auto &pacer = pacers.emplace_back(make_unique<UecPullPacer>(linkspeed, 0.99,
          UecBasePacket::unquantize(UecSink::_credit_per_pull), eventlist, ports));

        if (UecSink::_model_pcie)
            pcie_models.push_back(new PCIeModel(linkspeed * pcie_rate, UecSrc::_mtu, eventlist,
              pacer.get()));

        if (UecSink::_oversubscribed_cc)
            oversubscribed_ccs.push_back(new OversubscribedCC(eventlist, pacer.get()));

        auto &nic = nics.emplace_back(make_unique<UecNIC>(ix, eventlist,
                                                          linkspeed, ports));
        if (log_nic) {
            nic_logger->monitorNic(nic.get());
        }
    }

    // used just to print out stats data at the end
    list <const Route*> routes;

    vector<connection*>* all_conns = conns->getAllConnections();
    vector <UecSrc*> uec_srcs;

    map<flowid_t, pair<UecSrc*, UecSink*>> flowmap;
    map<flowid_t, UecPdcSes*> flow_pdc_map;
    if(planes != 1){
        cout << "We are taking the plane 0 to calculate the network rtt; If all the planes have the same tiers, you can remove this check." << endl;
        assert(false);
    }

    if(eventlist.isEmpty()){
        cout<<"EventList VUOTA 1"<<endl;
    }else{
        cout<<"EventList NON VUOTA 1"<<endl;
    }

    mem_b cwnd_b = cwnd*Packet::data_packet_size();
    if(sa){

        if(process == 1){
            int src = 0;  
            int dest = 16;                          // fuori range: i pacchetti finiranno al tap  
            uint32_t sink_host = src;               // sink "virtuale" agganciato alla NIC di src (o altro nodo valido)  
            
            simtime_picosec base_rtt_bw_two_points = 0;  
            
            // 1. multipath — invariato  
            unique_ptr<UecMultipath> mp = nullptr;  
            if (load_balancing_algo == BITMAP)  
                mp = make_unique<UecMpBitmap>(path_entropy_size, UecSrc::_debug);  
            else if (load_balancing_algo == REPS)  
                mp = make_unique<UecMpReps>(path_entropy_size, UecSrc::_debug, !disable_trim);  
            else if (load_balancing_algo == OBLIVIOUS)  
                mp = make_unique<UecMpOblivious>(path_entropy_size, UecSrc::_debug);  
            else if (load_balancing_algo == MIXED)  
                mp = make_unique<UecMpMixed>(path_entropy_size, UecSrc::_debug);  
            else { cout << "ERROR: multipath\n"; abort(); }  
            
            // 2. src e sink — il sink usa la NIC/pacer di sink_host, NON di dest  
            UecSrc* uec_src = new UecSrc(traffic_logger, eventlist, move(mp), *nics.at(src), ports);  
            UecSink* uec_snk;  
            if (receiver_driven)  
                uec_snk = new UecSink(NULL, pacers[sink_host].get(), *nics.at(sink_host), ports);  
            else  
                uec_snk = new UecSink(NULL, linkspeed, 1.1,  
                    UecBasePacket::unquantize(UecSink::_credit_per_pull), eventlist, *nics.at(sink_host), ports);  
            
            uec_src->setFlowId(123);   // stesso valore su entrambi i lati  
            uec_snk->setFlowId(123);

            // 3. CC — invariato  
            if (receiver_driven)  
                uec_src->initRccc(cwnd_b, enable_accurate_base_rtt ? base_rtt_bw_two_points : network_max_unloaded_rtt);  
            if (sender_driven)  
                uec_src->initNscc(cwnd_b, enable_accurate_base_rtt ? base_rtt_bw_two_points : network_max_unloaded_rtt);  
            
            uec_srcs.push_back(uec_src);  
            uec_src->setDst(dest);                  // 16: è questo che triggera il redirect al tap nel CORE  
            uec_src->setName("Uec_0_16");  
            logfile.writeName(*uec_src);  
            uec_snk->setSrc(src);  
            flowmap[uec_src->flowId()] = { uec_src, uec_snk };  
            ((DataReceiver*)uec_snk)->setName("Uec_sink_0_16");  
            logfile.writeName(*(DataReceiver*)uec_snk);  
            
            if (UecSink::_model_pcie)      uec_snk->setPCIeModel(pcie_models[sink_host]);  
            if (UecSink::_oversubscribed_cc) uec_snk->setOversubscribedCC(oversubscribed_ccs[sink_host]);  
            
            uec_src->setFlowsize(2000000);  
            
            // 4. route + registrazione ToR  
            for (uint32_t p = 0; p < planes; p++) {  
                Route* srctotor = new Route();  
                srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);  
                srctotor->push_back(topo[p]->pipes_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);  
                srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());  
            
                // routeback: deve essere una Route valida ma non verrà mai percorsa  
                // (il sink non riceve nulla → non manda ACK). Riuso la route verso src.  
                Route* dsttotor = new Route();  
                dsttotor->push_back(topo[p]->queues_ns_nlp[sink_host][topo_cfg->HOST_POD_SWITCH(sink_host)][0]);  
                dsttotor->push_back(topo[p]->pipes_ns_nlp[sink_host][topo_cfg->HOST_POD_SWITCH(sink_host)][0]);  
                dsttotor->push_back(topo[p]->queues_ns_nlp[sink_host][topo_cfg->HOST_POD_SWITCH(sink_host)][0]->getRemoteEndpoint());  
            
                uec_src->connectPort(p, *srctotor, *dsttotor, *uec_snk, 0);  
            
                // registra SOLO la porta del src: addHostPort(dest=16) accederebbe a queues_nlp_ns[...][16] OOB  
                topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]->addHostPort(src, uec_snk->flowId(), uec_src->getPort(p));
            }  
            
            if (log_sink)        sink_logger->monitorSink(uec_snk);  
            if (log_flow_events) uec_src->logFlowEvents(*event_logger);
        }else if(process == 0){
            int local_host = 0;
            int global_host = 16;
            int remote_src = 0;
            
            // src fantasma: non invia nulla, serve perché UecSink dereferenzia _src  
            unique_ptr<UecMultipath> mp = make_unique<UecMpOblivious>(path_entropy_size, false);  
            UecSrc* ghost_src = new UecSrc(traffic_logger, eventlist, move(mp), *nics.at(local_host), ports);  
            
            UecSink* real_snk;  
            if (receiver_driven)  
                real_snk = new UecSink(NULL, pacers[local_host].get(), *nics.at(local_host), ports);  
            else  
                real_snk = new UecSink(NULL, linkspeed, 1.1,  
                    UecBasePacket::unquantize(UecSink::_credit_per_pull), eventlist, *nics.at(local_host), ports);  
            
            // indirizzo di ritorno = dove andranno gli ACK (out-of-range su B → tap di B)  
            ghost_src->setDst(global_host);           // dst out-of-range DI B  
            real_snk->setSrc(remote_src);    // usato in pull()/newpkt come destinazione dei pacchetti di ritorno  
            
            ghost_src->setFlowId(123);  
            real_snk->setFlowId(123);

            ghost_src->setFlowsize(2000000);
                    
            for (uint32_t p = 0; p < planes; p++) {  
                Route* tohost = new Route();       // diventa il routeback del sink  
                tohost->push_back(topo[p]->queues_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]);  
                tohost->push_back(topo[p]->pipes_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]);  
                tohost->push_back(topo[p]->queues_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]->getRemoteEndpoint());  
            
                Route* ghost_route = new Route();  // route del ghost src, mai usata davvero  
                ghost_route->push_back(topo[p]->queues_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]);  
                ghost_route->push_back(topo[p]->pipes_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]);  
                ghost_route->push_back(topo[p]->queues_ns_nlp[local_host][topo_cfg->HOST_POD_SWITCH(local_host)][0]->getRemoteEndpoint());  
            
                // NON passare start_time: qui nessuno deve partire da solo  
                ghost_src->connectPort(p, *ghost_route, *tohost, *real_snk, TRIGGER_START);
            
                // i pacchetti in entrata diretti a local_host con questo flow_id finiscono al sink  
                topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(local_host)]  
                    ->addHostPort(local_host, ghost_src->flowId(), real_snk->getPort(p));  
            }
        }
    }

    if(eventlist.isEmpty()){
        cout<<"EventList VUOTA 2"<<endl;
    }else{
        cout<<"EventList NON VUOTA 2"<<endl;
    }

    if(sa && false){
        for (size_t c = 0; c < all_conns->size(); c++){
            
        }
    }else if(!sa){
        for (size_t c = 0; c < all_conns->size(); c++){
            connection* crt = all_conns->at(c);
            int src = crt->src;
            int dest = crt->dst;

            if (!conn_reuse and crt->msgid.has_value()) {
                cout << "msg keyword can only be used when conn_reuse is enabled.\n";
                abort();
            }

            assert(planes > 0);
            simtime_picosec transmission_delay = (Packet::data_packet_size() * 8 / speedAsGbps(linkspeed) * topo_cfg->get_diameter() * 1000) 
                                                + (UecBasePacket::get_ack_size() * 8 / speedAsGbps(linkspeed) * topo_cfg->get_diameter() * 1000);
            simtime_picosec base_rtt_bw_two_points = 2*topo_cfg->get_two_point_diameter_latency(src, dest) + transmission_delay;

            //cout << "Connection " << crt->src << "->" <<crt->dst << " starting at " << crt->start << " size " << crt->size << endl;

            if (!conn_reuse 
                || (crt->flowid and flowmap.find(crt->flowid) == flowmap.end())) {
                unique_ptr<UecMultipath> mp = nullptr;
                if (load_balancing_algo == BITMAP){
                    mp = make_unique<UecMpBitmap>(path_entropy_size, UecSrc::_debug);
                } else if (load_balancing_algo == REPS){
                    mp = make_unique<UecMpReps>(path_entropy_size, UecSrc::_debug, !disable_trim);
                } else if (load_balancing_algo == REPS_LEGACY){
                    mp = make_unique<UecMpRepsLegacy>(path_entropy_size, UecSrc::_debug);
                }else if (load_balancing_algo == OBLIVIOUS){
                    mp = make_unique<UecMpOblivious>(path_entropy_size, UecSrc::_debug);
                } else if (load_balancing_algo == MIXED){
                    mp = make_unique<UecMpMixed>(path_entropy_size, UecSrc::_debug);
                } else {
                    cout << "ERROR: Failed to set multipath algorithm, abort." << endl;
                    abort();
                }

                uec_src = new UecSrc(traffic_logger, eventlist, move(mp), *nics.at(src), ports);

                if (crt->flowid) {
                    uec_src->setFlowId(crt->flowid);
                    assert(flowmap.find(crt->flowid) == flowmap.end()); // don't have dups
                }

                if (conn_reuse) {
                    stringstream uec_src_dbg_tag;
                    uec_src_dbg_tag << "flow_id " << uec_src->flowId();
                    UecPdcSes* pdc = new UecPdcSes(uec_src, EventList::getTheEventList(), UecSrc::_mss, UecSrc::_hdr_size, uec_src_dbg_tag.str());
                    uec_src->makeReusable(pdc);
                    flow_pdc_map[uec_src->flowId()] = pdc;
                }

                if (receiver_driven)
                    uec_snk = new UecSink(NULL, pacers[dest].get(), *nics.at(dest), ports);
                else //each connection has its own pacer, so receiver driven mode does not kick in! 
                    uec_snk = new UecSink(NULL,linkspeed,1.1,UecBasePacket::unquantize(UecSink::_credit_per_pull),eventlist,*nics.at(dest), ports);

                flowmap[uec_src->flowId()] = { uec_src, uec_snk };

                if (crt->flowid) {
                    uec_snk->setFlowId(crt->flowid);
                }

                // If cwnd is 0 initXXcc will set a sensible default value 
                if (receiver_driven) {
                    // uec_src->setCwnd(cwnd*Packet::data_packet_size());
                    // uec_src->setMaxWnd(cwnd*Packet::data_packet_size());

                    if (enable_accurate_base_rtt) {
                        uec_src->initRccc(cwnd_b, base_rtt_bw_two_points);
                    } else {
                        uec_src->initRccc(cwnd_b, network_max_unloaded_rtt);
                    }
                }

                if (sender_driven) {
                    if (enable_accurate_base_rtt) {
                        uec_src->initNscc(cwnd_b, base_rtt_bw_two_points);
                    } else {
                        uec_src->initNscc(cwnd_b, network_max_unloaded_rtt);
                    }
                }
                uec_srcs.push_back(uec_src);
                uec_src->setDst(dest);

                if (log_flow_events) {
                    uec_src->logFlowEvents(*event_logger);
                }
                

                uec_src->setName("Uec_" + ntoa(src) + "_" + ntoa(dest));
                logfile.writeName(*uec_src);
                uec_snk->setSrc(src);

                if (UecSink::_model_pcie){
                    uec_snk->setPCIeModel(pcie_models[dest]);
                }
                                
                if (UecSink::_oversubscribed_cc){
                    uec_snk->setOversubscribedCC(oversubscribed_ccs[dest]);
                }

                ((DataReceiver*)uec_snk)->setName("Uec_sink_" + ntoa(src) + "_" + ntoa(dest));
                logfile.writeName(*(DataReceiver*)uec_snk);

                if (!conn_reuse) {
                    if (crt->size>0){
                        uec_src->setFlowsize(crt->size);
                    }

                    if (crt->trigger) {
                        Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                        trig->add_target(*uec_src);
                    }

                    if (crt->send_done_trigger) {
                        Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                        uec_src->setEndTrigger(*trig);
                    }

                    if (crt->recv_done_trigger) {
                        Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                        uec_snk->setEndTrigger(*trig);
                    }
                } else {
                    assert(crt->size > 0);

                    optional<simtime_picosec> start_ts = {};
                    if (crt->start != TRIGGER_START) {
                        start_ts.emplace(timeFromUs((uint32_t)crt->start));
                    } 

                    UecPdcSes* pdc = flow_pdc_map.find(crt->flowid)->second;
                    UecMsg* msg = pdc->enque(crt->size, start_ts, true);

                    if (crt->trigger) {
                        Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                        trig->add_target(*msg);
                    }

                    if (crt->send_done_trigger) {
                        Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                        msg->setTrigger(UecMsg::MsgStatus::SentLast, trig);
                    }

                    if (crt->recv_done_trigger) {
                        Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                        uec_snk->setEndTrigger(*trig);
                        msg->setTrigger(UecMsg::MsgStatus::RecvdLast, trig);
                    }
                }

                //uec_snk->set_priority(crt->priority);
                                
                for (uint32_t p = 0; p < planes; p++) {
                    switch (route_strategy) {
                    case ECMP_FIB:
                    case ECMP_FIB_ECN:
                    case REACTIVE_ECN:
                        {
                            Route* srctotor = new Route();
                            srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                            srctotor->push_back(topo[p]->pipes_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]);
                            srctotor->push_back(topo[p]->queues_ns_nlp[src][topo_cfg->HOST_POD_SWITCH(src)][0]->getRemoteEndpoint());

                            Route* dsttotor = new Route();
                            dsttotor->push_back(topo[p]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                            dsttotor->push_back(topo[p]->pipes_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]);
                            dsttotor->push_back(topo[p]->queues_ns_nlp[dest][topo_cfg->HOST_POD_SWITCH(dest)][0]->getRemoteEndpoint());

                            uec_src->connectPort(p, *srctotor, *dsttotor, *uec_snk, crt->start);
                            //uec_src->setPaths(path_entropy_size);
                            //uec_snk->setPaths(path_entropy_size);

                            //register src and snk to receive packets from their respective TORs. 
                            assert(topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]);
                            assert(topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]);
                            topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(src)]->addHostPort(src,uec_snk->flowId(),uec_src->getPort(p));
                            topo[p]->switches_lp[topo_cfg->HOST_POD_SWITCH(dest)]->addHostPort(dest,uec_src->flowId(),uec_snk->getPort(p));
                            break;
                        }
                    default:
                        abort();
                    }
                }

                // set up the triggers
                // xxx

                if (log_sink) {
                    sink_logger->monitorSink(uec_snk);
                }
            } else {
                // Use existing connection for this message
                assert(crt->msgid.has_value());

                UecPdcSes* pdc = flow_pdc_map.find(crt->flowid)->second;
                uec_src = nullptr;
                uec_snk = nullptr;

                optional<simtime_picosec> start_ts = {};
                if (crt->start != TRIGGER_START) {
                    start_ts.emplace(timeFromUs((uint32_t)crt->start));
                } 

                UecMsg* msg = pdc->enque(crt->size, start_ts, true);

                if (crt->trigger) {
                    Trigger* trig = conns->getTrigger(crt->trigger, eventlist);
                    trig->add_target(*msg);
                }

                if (crt->send_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->send_done_trigger, eventlist);
                    msg->setTrigger(UecMsg::MsgStatus::SentLast, trig);
                }

                if (crt->recv_done_trigger) {
                    Trigger* trig = conns->getTrigger(crt->recv_done_trigger, eventlist);
                    msg->setTrigger(UecMsg::MsgStatus::RecvdLast, trig);
                }
            }
        }
    }
    
    
    Logged::dump_idmap();
    // Record the setup
    int pktsize = Packet::data_packet_size();
    logfile.write("# pktsize=" + ntoa(pktsize) + " bytes");
    logfile.write("# hostnicrate = " + ntoa(linkspeed/1000000) + " Mbps");
    //logfile.write("# corelinkrate = " + ntoa(HOST_NIC*CORE_TO_HOST) + " pkt/sec");
    //logfile.write("# buffer = " + ntoa((double) (queues_na_ni[0][1]->_maxsize) / ((double) pktsize)) + " pkt");
    
    // GO!
    if(sa){
        //sync all child TEMP
        raise(SIGSTOP);


        //initial updating of guide
        EventHeaderUpdater update;
        update.child_id = process;

        strcpy(update.message, "INIT");
        update.next_event_time =(eventlist.isEmpty()) ? 0 : eventlist.peekNextEventTime();

        socket.send(zmq::buffer(&update, sizeof(update)), zmq::send_flags::none);

        //GO!
        cout<<"Ready to simulate... waiting GUIDE"<<endl;
        EventTimeLimit arg;
        do{
            socket.recv(resp, zmq::recv_flags::none);
            
            if(resp.size() != sizeof(EventTimeLimit)){
                cerr<<"Unaspected response, shutting down..."<<endl;
                exit(EXIT_FAILURE);
            }
            
            memcpy(&arg, resp.data(), sizeof(EventTimeLimit));

            if(strcmp(arg.message, "STOP") == 0) break;
            if(strcmp(arg.message, "NOLIMIT") == 0){
                //cout<<"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAANOLIMIT"<<endl;
                while(!eventlist.isEmpty() && !eventlist.isStopped()){
                    eventlist.doNextEvent();
                }

            }else if(strcmp(arg.message, "GO") == 0){
                if(arg.time_limit < eventlist.now()){
                    cerr<<"Incongrous time detected, shutting down..."<<endl;
                    exit(EXIT_FAILURE);
                }

                //cout<<"Simulating until "<<timeAsUs(arg.time_limit)<<endl;
                while(eventlist.peekNextEventTime() <= arg.time_limit && !eventlist.isEmpty() && !eventlist.isStopped()){
                    eventlist.doNextEvent();
                }
            }else if(strcmp(arg.message, "NEW") == 0){
                socket.recv(resp, zmq::recv_flags::none);

                if(resp.size() != sizeof(SerializedUecPacket)){
                    cerr<<"Unaspected data, shutting down..."<<endl;
                    err_guide_routine();
                }

                SerializedUecPacket pkt;
                memcpy(&pkt, resp.data(), sizeof(SerializedUecPacket));
                //cout<<"Pkt succesfully delivered with destination "<<pkt.dst<<" with time "<< timeAsUs(pkt.time_of_event)<<endl;

                tap->recvFromWan(pkt);
            }else{
                cerr<<"Unaspected response, shutting down..."<<endl;
                exit(EXIT_FAILURE);
            }

            if(!eventlist.isEmpty()){
                //cout<<"UPDATEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"<<endl;
                strcpy(update.message, "UPDATE");
                update.next_event_time = eventlist.peekNextEventTime();
            }else{
                //cout<<"DONEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE"<<endl;
                strcpy(update.message, "DONE");
                update.next_event_time = 0;
            }
            
            if(eventlist.isStopped()){
                eventlist.go();
            }
            //cout<<"sto inviando id:"<<update.child_id<<", message:"<<update.message<<", time:"<<update.next_event_time<<endl;
            socket.send(zmq::buffer(&update, sizeof(EventHeaderUpdater)), zmq::send_flags::none);

        }while(true);

        cout<<"Simulation ended"<<endl;
    }else{
        cout << "Starting simulation" << endl;

        while (eventlist.doNextEvent()) {}
        cout << "Done" << endl;
    }


    int new_pkts = 0, rtx_pkts = 0, bounce_pkts = 0, rts_pkts = 0, ack_pkts = 0, nack_pkts = 0, pull_pkts = 0, sleek_pkts = 0;
    for (size_t ix = 0; ix < uec_srcs.size(); ix++) {
        const struct UecSrc::Stats& s = uec_srcs[ix]->stats();
        new_pkts += s.new_pkts_sent;
        rtx_pkts += s.rtx_pkts_sent;
        rts_pkts += s.rts_pkts_sent;
        bounce_pkts += s.bounces_received;
        ack_pkts += s.acks_received;
        nack_pkts += s.nacks_received;
        pull_pkts += s.pulls_received;
        sleek_pkts += s._sleek_counter;
    }
    cout << "New: " << new_pkts << " Rtx: " << rtx_pkts << " RTS: " << rts_pkts << " Bounced: " << bounce_pkts << " ACKs: " << ack_pkts << " NACKs: " << nack_pkts << " Pulls: " << pull_pkts << " sleek_pkts: " << sleek_pkts << endl;
    
    
    /*
    list <const Route*>::iterator rt_i;
    int counts[10]; int hop;
    for (int i = 0; i < 10; i++)
        counts[i] = 0;
    cout << "route count: " << routes.size() << endl;
    for (rt_i = routes.begin(); rt_i != routes.end(); rt_i++) {
        const Route* r = (*rt_i);
        //print_route(*r);
#ifdef PRINTPATHS
        cout << "Path:" << endl;
#endif
        hop = 0;
        for (int i = 0; i < r->size(); i++) {
            PacketSink *ps = r->at(i); 
            CompositeQueue *q = dynamic_cast<CompositeQueue*>(ps);
            if (q == 0) {
#ifdef PRINTPATHS
                cout << ps->nodename() << endl;
#endif
            } else {
#ifdef PRINTPATHS
                cout << q->nodename() << " " << q->num_packets() << "pkts " 
                     << q->num_headers() << "hdrs " << q->num_acks() << "acks " << q->num_nacks() << "nacks " << q->num_stripped() << "stripped"
                     << endl;
#endif
                counts[hop] += q->num_stripped();
                hop++;
            }
        } 
#ifdef PRINTPATHS
        cout << endl;
#endif
    }
    for (int i = 0; i < 10; i++)
        cout << "Hop " << i << " Count " << counts[i] << endl;
    */  

    return EXIT_SUCCESS;
}

