
import da
PatternExpr_0 = da.pat.TuplePattern([da.pat.ConstantPattern('getBalance'), da.pat.FreePattern('request')])
PatternExpr_1 = da.pat.FreePattern('client')
PatternExpr_2 = da.pat.TuplePattern([da.pat.ConstantPattern('deposit'), da.pat.FreePattern('request')])
PatternExpr_3 = da.pat.FreePattern('client')
PatternExpr_4 = da.pat.TuplePattern([da.pat.ConstantPattern('withdraw'), da.pat.FreePattern('request')])
PatternExpr_5 = da.pat.FreePattern('client')
PatternExpr_6 = da.pat.TuplePattern([da.pat.ConstantPattern('ack'), da.pat.FreePattern('request')])
PatternExpr_7 = da.pat.FreePattern('next')
PatternExpr_8 = da.pat.TuplePattern([da.pat.ConstantPattern('propagate'), da.pat.FreePattern('request')])
PatternExpr_9 = da.pat.FreePattern('server')
PatternExpr_10 = da.pat.TuplePattern([da.pat.ConstantPattern('sync'), da.pat.FreePattern('request')])
PatternExpr_11 = da.pat.FreePattern('server')
PatternExpr_12 = da.pat.TuplePattern([da.pat.ConstantPattern('server_fail'), da.pat.FreePattern('request')])
PatternExpr_13 = da.pat.FreePattern('master')
PatternExpr_14 = da.pat.TuplePattern([da.pat.ConstantPattern('purge_req'), da.pat.FreePattern('request')])
PatternExpr_15 = da.pat.FreePattern('master')
PatternExpr_16 = da.pat.TuplePattern([da.pat.ConstantPattern('sync_new_tail'), da.pat.FreePattern('request')])
PatternExpr_17 = da.pat.FreePattern('master')
PatternExpr_18 = da.pat.TuplePattern([da.pat.ConstantPattern('update_history'), da.pat.FreePattern('request')])
PatternExpr_19 = da.pat.FreePattern('server')
PatternExpr_20 = da.pat.TuplePattern([da.pat.ConstantPattern('sent_done')])
PatternExpr_21 = da.pat.FreePattern('server')
PatternExpr_22 = da.pat.TuplePattern([da.pat.ConstantPattern('add_new_tail'), da.pat.FreePattern('req')])
PatternExpr_23 = da.pat.FreePattern('master')
PatternExpr_24 = da.pat.TuplePattern([da.pat.ConstantPattern('new_tail_fail'), da.pat.FreePattern('req')])
PatternExpr_25 = da.pat.FreePattern('master')
PatternExpr_26 = da.pat.TuplePattern([da.pat.ConstantPattern('servers_list'), da.pat.FreePattern('req')])
PatternExpr_27 = da.pat.FreePattern('master')
PatternExpr_28 = da.pat.TuplePattern([da.pat.FreePattern(None)])
PatternExpr_29 = da.pat.FreePattern('p')
PatternExpr_30 = da.pat.TuplePattern([da.pat.ConstantPattern('response'), da.pat.FreePattern('req')])
PatternExpr_31 = da.pat.FreePattern('server')
PatternExpr_32 = da.pat.TuplePattern([da.pat.ConstantPattern('update_head'), da.pat.FreePattern('req')])
PatternExpr_33 = da.pat.FreePattern('master')
PatternExpr_34 = da.pat.TuplePattern([da.pat.ConstantPattern('update_tail'), da.pat.FreePattern('req')])
PatternExpr_35 = da.pat.FreePattern('master')
PatternExpr_36 = da.pat.TuplePattern([da.pat.ConstantPattern('heartbeat')])
PatternExpr_37 = da.pat.FreePattern('server')
PatternExpr_38 = da.pat.TuplePattern([da.pat.ConstantPattern('new_tail_ready')])
PatternExpr_39 = da.pat.FreePattern('server')
import sys
import random
import logging
from time import sleep
exec(('from %s import *' % sys.argv[3]), globals())

class Server(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_0', PatternExpr_0, sources=[PatternExpr_1], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_0]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_1', PatternExpr_2, sources=[PatternExpr_3], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_1]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_2', PatternExpr_4, sources=[PatternExpr_5], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_2]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_3', PatternExpr_6, sources=[PatternExpr_7], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_3]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_4', PatternExpr_8, sources=[PatternExpr_9], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_4]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_5', PatternExpr_10, sources=[PatternExpr_11], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_5]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_6', PatternExpr_12, sources=[PatternExpr_13], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_6]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_7', PatternExpr_14, sources=[PatternExpr_15], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_7]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_8', PatternExpr_16, sources=[PatternExpr_17], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_8]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_9', PatternExpr_18, sources=[PatternExpr_19], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_9]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_10', PatternExpr_20, sources=[PatternExpr_21], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_10]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_11', PatternExpr_22, sources=[PatternExpr_23], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_11]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_12', PatternExpr_24, sources=[PatternExpr_25], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_12]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_13', PatternExpr_26, sources=[PatternExpr_27], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_13]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ServerReceivedEvent_14', PatternExpr_28, sources=[PatternExpr_29], destinations=None, timestamps=None, record_history=None, handlers=[self._Server_handler_14])])

    def setup(self, servers_set, bank_name, index, master):
        self.bank_name = bank_name
        self.servers_set = servers_set
        self.master = master
        self.index = index
        self.startup_delay = server_conf[self.bank_name][self.index]['startup_delay']
        self.rcv_lifetime = server_conf[self.bank_name][self.index]['rcv_lifetime']
        self.snd_lifetime = server_conf[self.bank_name][self.index]['snd_lifetime']
        self.heartbeat_interval = server_conf[self.bank_name][self.index]['heartbeat_interval']
        self.client_update_req = False
        self.syncing_with_tail = False
        self.query_req = False
        self.server_propagate_req = False
        self.ack_resp = False
        self.history_list = []
        self.query_queue = []
        self.update_queue = []
        self.sent_queue = []
        self.ack_queue = []
        self.propagate_queue = []
        self.servers = list(self.servers_set)
        self.recv_seq_no = 0
        self.send_seq_no = 0
        self.server_name = ((self.bank_name + '.') + str(server_conf[self.bank_name][self.index]['index']))
        self.seq = 0
        self.last_seen_update_seq = 0
        self.head = self.servers[0]
        self.tail = self.servers[(- 1)]
        self.state_active = True
        self.new_server = None
        self.update_index = 0
        self.update_history_in_progress = False
        self.initialize = True
        if (server_conf[self.bank_name][self.index]['rcv_lifetime'] == 'random'):
            self.rcv_lifetime = random.randint(50, 100)
        else:
            self.rcv_lifetime = server_conf[self.bank_name][self.index]['rcv_lifetime']
        if (server_conf[self.bank_name][self.index]['snd_lifetime'] == 'random'):
            self.send_lifetime = random.randint(50, 100)
        else:
            self.send_lifetime = server_conf[self.bank_name][self.index]['snd_lifetime']
        self.output(self.server_name, 'Receive Lifetime =', self.rcv_lifetime, 'Send Lifetime =', self.send_lifetime)

    def main(self):
        if self.startup_delay:
            self.output(self.server_name, 'Sleeping for ', self.startup_delay, 'seconds')
            self.state_active = False
            sleep(self.startup_delay)
            self.initialize = False
        while True:
            super()._label('l', block=False)
            _st_label_347 = 0
            self._timer_start()
            while (_st_label_347 == 0):
                _st_label_347 += 1
                if (self.client_update_req or self.query_req or self.ack_resp or self.server_propagate_req or self.update_history_in_progress):
                    if (self.initialize == False):
                        self.client_update_req = False
                        self.syncing_with_tail = False
                        self.query_req = False
                        self.server_propagate_req = False
                        self.ack_resp = False
                        self.history_list = []
                        self.query_queue = []
                        self.update_queue = []
                        self.sent_queue = []
                        self.ack_queue = []
                        self.propagate_queue = []
                    if ((self.recv_seq_no + 1) == self.rcv_lifetime):
                        self.output(self.server_name, 'Server Recv Lifetime Exceeded, Killing Self..')
                        return
                    if (self.send_seq_no == self.rcv_lifetime):
                        self.output(self.server_name, 'Server Send Lifetime Exceeded, Killing Self..')
                        return
                    if self.client_update_req:
                        self.output(self.server_name, 'Handle Client Updates')
                        self.handle_update_propagate_request(True)
                        self.client_update_req = False
                    elif self.query_req:
                        self.output(self.server_name, 'Handle Client Queries')
                        self.handle_queries()
                        self.query_req = False
                    elif self.ack_resp:
                        self.output(self.server_name, 'Handle ACK responses')
                        self.handle_ack_responses()
                        self.ack_resp = False
                    elif self.server_propagate_req:
                        self.output(self.server_name, 'Handle Propagate requests')
                        self.handle_update_propagate_request(False)
                        self.server_propagate_req = False
                    elif self.update_history_in_progress:
                        self.output(self.server_name, 'Updating History in Progress')
                        self.handle_update_history_sends()
                    _st_label_347 += 1
                elif self._timer_expired:
                    self.output(self.server_name, 'Send Heartbeat msg to', self.master)
                    self._send(('heartbeat',), self.master)
                    _st_label_347 += 1
                else:
                    super()._label('l', block=True, timeout=self.heartbeat_interval)
                    _st_label_347 -= 1
            else:
                if (_st_label_347 != 2):
                    continue
            if (_st_label_347 != 2):
                break

    def get_balance(self, request):
        " request format=(UID, account_number)\n            returns (UID, account_number, 'Processed', balance)\n        "
        account_data = [item for item in self.history_list if (item[1][1] == request[1])]
        if account_data:
            return (request[0], request[1], 'Processed', account_data[(- 1)][1][2])
        else:
            return (request[0], request[1], 'Processed', 0)

    def handle_queries(self):
        for entry in list(self.query_queue):
            self.send_seq_no += 1
            response = ('response', self.get_balance(entry[0]))
            self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
            self._send(response, entry[(- 1)])
            self.query_queue.remove(entry)

    def handle_update_propagate_request(self, update):
        if update:
            current_queue = self.update_queue
        else:
            current_queue = self.propagate_queue
        for (i, entry) in enumerate(list(current_queue)):
            self.send_seq_no += 1
            if (not (self.tail == self.id)):
                self.send_seq_no += 1
                propagate = ('propagate', entry)
                self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', propagate, ' to', str(self.servers[(self.index + 1)]))
                self._send(propagate, self.servers[(self.index + 1)])
                self.sent_queue.append(entry)
            else:
                dup_req = [item for item in self.history_list if ((item[1][0] == entry[2][0]) and (item[1][1] == entry[2][1]))]
                if dup_req:
                    consistent_hist = [item for item in dup_req if ((item[1][2] == entry[2][2]) and (item[0] == entry[1]))]
                    if consistent_hist:
                        response = (entry[2][0], entry[2][1], 'Processed', entry[2][2])
                        self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                        self._send(('response', response), entry[(- 1)])
                    else:
                        response = (entry[2][0], entry[2][1], 'Inconsistent With History', dup_req[(- 1)][1][2])
                        self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                        self._send(('response', response), entry[(- 1)])
                else:
                    existing_entry = [item for item in self.history_list if (item[1][1] == entry[2][1])]
                    if existing_entry:
                        if (entry[1] == 'deposit'):
                            mutable_entry = list(entry[2])
                            mutable_entry[2] += existing_entry[(- 1)][1][2]
                            self.history_list.append(('deposit', tuple(mutable_entry)))
                            response = (mutable_entry[0], mutable_entry[1], 'Processed', mutable_entry[2])
                            self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                            self._send(('response', response), entry[(- 1)])
                        elif (entry[2][2] <= existing_entry[(- 1)][1][2]):
                            mutable_entry = list(entry[2])
                            mutable_entry[2] = (existing_entry[(- 1)][1][2] - entry[2][2])
                            self.history_list.append(('withdraw', tuple(mutable_entry)))
                            response = (mutable_entry[0], mutable_entry[1], 'Processed', mutable_entry[2])
                            self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                            self._send(('response', response), entry[(- 1)])
                        else:
                            response = (entry[2][0], entry[2][1], 'Insufficient Funds', existing_entry[(- 1)][1][2])
                            self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                            self._send(('response', response), entry[(- 1)])
                    elif (entry[1] == 'deposit'):
                        self.history_list.append((entry[1], entry[2]))
                        response = (entry[2][0], entry[2][1], 'Processed', entry[2][2])
                        self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                        self._send(('response', response), entry[(- 1)])
                    else:
                        response = (entry[2][0], entry[2][1], 'Insufficient Funds', 0)
                        self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', response, ' to', str(entry[(- 1)]))
                        self._send(('response', response), entry[(- 1)])
                if (not (0 == self.index)):
                    self.send_seq_no += 1
                    ack = ('ack', (entry[0], entry[1], response))
                    self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', ack, ' to', str(self.servers[(self.index - 1)]))
                    self._send(ack, self.servers[(self.index - 1)])
            current_queue.remove(entry)
        self.output(self.server_name, 'History:\n', self.history_list)

    def handle_ack_responses(self):
        'Routine to Handle ACK responses from the Successor Node\n        '
        for request in list(self.ack_queue):
            lost_acks = [item for item in self.sent_queue if (item[0] < request[0])]
            self.output('lost ACKs = ', lost_acks)
            if lost_acks:
                lost_acks.sort()
                for entry in lost_acks:
                    dup_req = [item for item in self.history_list if ((item[1][0] == entry[2][0]) and (item[1][1] == entry[2][1]))]
                    if dup_req:
                        consistent_hist = [item for item in dup_req if ((item[1][2] == entry[2][2]) and (item[0] == entry[1]))]
                        if consistent_hist:
                            response = (entry[2][0], entry[2][1], 'Processed', entry[2][2])
                        else:
                            response = (entry[2][0], entry[2][1], 'Inconsistent With History', dup_req[(- 1)][1][2])
                    else:
                        existing_entry = [item for item in self.history_list if (item[1][1] == entry[2][1])]
                        if existing_entry:
                            if (entry[1] == 'deposit'):
                                mutable_entry = list(entry[2])
                                mutable_entry[2] += existing_entry[(- 1)][1][2]
                                self.history_list.append(('deposit', tuple(mutable_entry)))
                                response = (mutable_entry[0], mutable_entry[1], 'Processed', mutable_entry[2])
                            elif (entry[2][2] <= existing_entry[(- 1)][1][2]):
                                mutable_entry = list(entry[2])
                                mutable_entry[2] = (existing_entry[(- 1)][1][2] - entry[2][2])
                                self.history_list.append(('withdraw', tuple(mutable_entry)))
                                response = (mutable_entry[0], mutable_entry[1], 'Processed', mutable_entry[2])
                            else:
                                response = (entry[2][0], entry[2][1], 'Insufficient Funds', existing_entry[(- 1)][1][2])
                        elif (entry[1] == 'deposit'):
                            self.history_list.append((entry[1], entry[2]))
                            response = (entry[2][0], entry[2][1], 'Processed', entry[2][2])
                        else:
                            response = (entry[2][0], entry[2][1], 'Insufficient Funds', 0)
                    if (not (0 == self.index)):
                        self.send_seq_no += 1
                        ack = ('ack', (entry[0], entry[1], response))
                        self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', ack, ' to', str(self.servers[(self.index - 1)]))
                        self._send(ack, self.servers[(self.index - 1)])
                    self.sent_queue.remove(entry)
        for request in list(self.ack_queue):

            def findMatch(candidate):
                return ((request[0] == candidate[0]) and (request[1] == candidate[1]) and (request[2][0] == candidate[2][0]) and (request[2][1] == candidate[2][1]))
            try:
                entry = tuple(filter(findMatch, self.sent_queue))[0]
            except Exception:
                self.output(self.server_name, 'Found Stray ACK:', request, ' ignoring')
                self.ack_queue.remove(request)
                return
            self.sent_queue.remove(entry)
            self.ack_queue.remove(request)
            if (request[2][2] == 'Processed'):
                dup_req = [item for item in self.history_list if (item[1][1] == request[2][1])]
                hist_entry = (request[1], (request[2][0], request[2][1], request[2][3]))
                if dup_req:
                    if (not (dup_req[(- 1)][1][2] == request[2][3])):
                        self.history_list.append(hist_entry)
                else:
                    self.history_list.append(hist_entry)
            if (not (0 == self.index)):
                self.send_seq_no += 1
                ack = ('ack', request)
                self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', ack, ' to', str(self.servers[(self.index - 1)]))
                self._send(('ack', request), self.servers[(self.index - 1)])
        self.output(self.server_name, 'History After ACK receipt is', self.history_list)

    def handle_update_history_sends(self):
        length = len(self.history_list)
        if (self.update_index <= length):
            self.send_seq_no += 1
            update_history = ('update_history', self.history_list[self.update_index])
            self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', update_history, ' to', self.new_server)
            self._send(update_history, self.new_server)
        self.update_index += 1
        if (self.update_index == length):
            self.update_history_in_progress = False
            self.send_seq_no += 1
            sent_done = ('sent_done',)
            self.output(self.server_name, 'SENT:', self.send_seq_no, ': sent_done: to', self.new_server)
            self._send(sent_done, self.new_server)
            self.tail = self.new_server
            self.servers.append(self.new_server)
            self.new_server = None

    def _Server_handler_0(self, request, client):
        self.recv_seq_no += 1
        if (self.id == self.tail):
            self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ', str(request), ' from', str(client))
            queue_entry = [request, client]
            self.query_queue.append(queue_entry)
            self.query_req = True
        else:
            del request
    _Server_handler_0._labels = None
    _Server_handler_0._notlabels = None

    def _Server_handler_1(self, client, request):
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( deposit,', request, ') from', str(client))
        self.seq += 1
        queue_entry = [self.seq, 'deposit', request, client]
        self.update_queue.append(queue_entry)
        self.client_update_req = True
    _Server_handler_1._labels = None
    _Server_handler_1._notlabels = None

    def _Server_handler_2(self, client, request):
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( withdraw,', request, ') from', str(client))
        self.seq += 1
        queue_entry = [self.seq, 'withdraw', request, client]
        self.update_queue.append(queue_entry)
        self.client_update_req = True
    _Server_handler_2._labels = None
    _Server_handler_2._notlabels = None

    def _Server_handler_3(self, request, next):
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( ack,', request, ') from', str(next))
        self.ack_queue.append(request)
        self.ack_resp = True
    _Server_handler_3._labels = None
    _Server_handler_3._notlabels = None

    def _Server_handler_4(self, server, request):
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( propagate,', request, ') from', str(server))
        self.propagate_queue.append(request)
        self.last_seen_update_seq = request[0]
        self.server_propagate_req = True
    _Server_handler_4._labels = None
    _Server_handler_4._notlabels = None

    def _Server_handler_5(self, request, server):
        'Routine to Send Inflight Updates to S+\n        '
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( sync: seq.no =', request, ') from', str(server))
        inflight_reqs = [item for item in self.sent_queue if (item[0] > request)]
        if inflight_reqs:
            inflight_reqs.sort()
            self.output(self.server_name, 'Resend List = ', inflight_reqs)
            for entry in inflight_reqs:
                self.send_seq_no += 1
                propagate = ('propagate', entry)
                self.output(self.server_name, 'SENT:', self.send_seq_no, ': resend', propagate, ' to', str(self.servers[(self.index + 1)]))
                self._send(propagate, self.servers[(self.index + 1)])
    _Server_handler_5._labels = None
    _Server_handler_5._notlabels = None

    def _Server_handler_6(self, request, master):
        ' Handle Server Fail messages from the Master\n        '
        self.output(self.server_name, 'RECEIVED: server_fail', request, 'from Master', master)
        if (self.id == request[0]):
            self.output(self.server_name, 'Master thinks I am dead, killing self..')
            self.exit(0)
        if (self.state_active == False):
            self.servers.remove(request[0])
            self.head = request[1]
            self.tail = request[2]
            return
        is_failed_pred = False
        if ((not (self.index == 0)) and (request[0] == self.servers[(self.index - 1)])):
            is_failed_pred = True
        internal_failure = True
        if ((not (self.id == self.head)) and (self.id == request[1])):
            self.seq = self.last_seen_update_seq
            internal_failure = False
        new_tail = False
        if ((not (self.id == self.tail)) and (self.id == request[2])):
            new_tail = True
        self.head = request[1]
        self.tail = request[2]
        self.servers.remove(request[0])
        for (i, elem) in enumerate(self.servers):
            if (elem == self.id):
                self.index = i
                break
        self.output(self.server_name, 'After Re-org, servers =', self.servers, 'Index =', self.index)
        if new_tail:
            internal_failure = False
            while (not (self.sent_queue == [])):
                self.output(self.server_name, 'remaining Sent Queue =', self.sent_queue)
                self.propagate_queue.append(self.sent_queue.pop(0))
            self.server_propagate_req = True
        if (internal_failure == True):
            if (is_failed_pred == True):
                self.send_seq_no += 1
                sync_msg = ('sync', self.last_seen_update_seq)
                self.output(self.server_name, 'SENT:', self.send_seq_no, ': ', sync_msg, 'to', str(self.servers[(self.index - 1)]))
                self._send(sync_msg, self.servers[(self.index - 1)])
        self.output('Sent Queue in Server Fail =', self.sent_queue)
    _Server_handler_6._labels = None
    _Server_handler_6._notlabels = None

    def _Server_handler_7(self, request, master):
        'Routine to Purge the partial Update in the *To be added Server* if the current tail failed\n        '
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( purge_req:', request, ') from', master)
        self.history_list = []
    _Server_handler_7._labels = None
    _Server_handler_7._notlabels = None

    def _Server_handler_8(self, request, master):
        'Start Syncing History with the new Server\n        '
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( sync_new_tail,', request, ') from', master)
        self.update_index = 0
        self.new_server = request
        self.update_history_in_progress = True
    _Server_handler_8._labels = None
    _Server_handler_8._notlabels = None

    def _Server_handler_9(self, request, server):
        'Keep adding to our history List\n        '
        self.recv_seq_no += 1
        if (self.recv_seq_no == self.rcv_lifetime):
            self.output(self.server_name, 'Server Recv Lifetime Exceeded, Killing Self..')
            self.exit(0)
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( Update History,', request, ') from', server)
        self.history_list.append(request)
    _Server_handler_9._labels = None
    _Server_handler_9._notlabels = None

    def _Server_handler_10(self, server):
        'The current tail has Sent us everything we need to start acting like a tail\n           Inform Master that I am the new tail and start acting like one\n        '
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ': ( Sent Done ) from', server)
        self.servers.append(self.id)
        self.tail = self.id
        self.state_active = True
        self.output(self.server_name, 'Updated History of New Tail =', self.history_list)
        self.output(self.server_name, 'Informing Master', self.master, 'that I am the new tail')
        self._send(('new_tail_ready',), self.master)
    _Server_handler_10._labels = None
    _Server_handler_10._notlabels = None

    def _Server_handler_11(self, req, master):
        'The new tail has taken over, update the server list and the tail\n        '
        self.output(self.server_name, '#RECEIVED: ', 'add_new_tail:', req, ' from Master ', master)
        if (not (self.tail == req)):
            self.output(self.server_name, 'Added new tail =', req)
            self.servers.append(req)
            self.tail = req
        else:
            self.output(self.server_name, 'Current Tail has already new tail')
    _Server_handler_11._labels = None
    _Server_handler_11._notlabels = None

    def _Server_handler_12(self, req, master):
        'The new tail has failed before taking over, update the server list and the tail\n        '
        self.output(self.server_name, '#RECEIVED: ', 'new_tail_fail:', req, ' from Master ', master)
        if (self.tail == req):
            self.output(self.server_name, 'Getting back to being Tail', self.id)
            self.servers.remove(req)
            self.tail = self.id
            while (not (self.sent_queue == [])):
                self.output(self.server_name, 'remaining Sent Queue =', self.sent_queue)
                self.propagate_queue.append(self.sent_queue.pop(0))
            self.server_propagate_req = True
        else:
            self.output(self.server_name, 'Stop History Propagation')
            self.update_history_in_progress = False
    _Server_handler_12._labels = None
    _Server_handler_12._notlabels = None

    def _Server_handler_13(self, req, master):
        'Inform the new server of our current server List, so that it has the latest \n           info and can re-organise the server list in response to a serv_fail event.\n        '
        self.output(self.server_name, '#RECEIVED: ', 'servers_list:', req, ' from Master ', master)
        self.servers = req
        self.head = self.servers[0]
        self.tail = self.servers[(- 1)]
        self.initialize = True
        self.output(self.server_name, 'Current Server List =', self.servers)
    _Server_handler_13._labels = None
    _Server_handler_13._notlabels = None

    def _Server_handler_14(self, p):
        self.recv_seq_no += 1
        self.output(self.server_name, 'RECEIVED:', self.recv_seq_no, ':', 'Unknown message from ', str(p), ' ,discarding..')
    _Server_handler_14._labels = None
    _Server_handler_14._notlabels = None

class Client(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_0', PatternExpr_30, sources=[PatternExpr_31], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_15]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_1', PatternExpr_32, sources=[PatternExpr_33], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_16]), da.pat.EventPattern(da.pat.ReceivedEvent, '_ClientReceivedEvent_2', PatternExpr_34, sources=[PatternExpr_35], destinations=None, timestamps=None, record_history=None, handlers=[self._Client_handler_17])])

    def setup(self, servers_set, bank_name, index, master):
        self.bank_name = bank_name
        self.servers_set = servers_set
        self.master = master
        self.index = index
        self.client_time_out = client_conf[self.bank_name][self.index]['client_time_out']
        self.num_retransmits = client_conf[self.bank_name][self.index]['num_retransmits']
        self.account_no = client_conf[self.bank_name][self.index]['account_no']
        self.msg_loss_freq = client_conf[self.bank_name][self.index]['msg_loss_freq']
        self.seq_no = random.randint(1000, 10000)
        self.server_reply = False
        self.servers = list(self.servers_set)
        self.send_seq_no = 0
        self.recv_seq_no = 1
        self.client_name = ((('Client.' + self.bank_name) + '.') + str(client_conf[self.bank_name][self.index]['index']))
        self.head = self.servers[0]
        self.tail = self.servers[(- 1)]

    def main(self):
        self.output('### Client', self.client_name, self.id, ' firing up...')
        if client_seq:
            for (i, req) in enumerate(client_seq):
                sleep(random.randint(0, 3))
                self.output(self.client_name, '### Running request number ', (i + 1))
                self.send_request(req)
        elif client_prob_conf:
            random.seed(client_prob_conf[self.index]['seed'])
            for i in range(client_prob_conf[self.index]['numReq']):
                sleep(random.randint(0, 3))
                self.output(self.client_name, '### Running request number ', (i + 1))
                req = self.get_random_req(client_prob_conf[self.index])
                self.send_request(req)

    def generate_uid(self, bank_name, account_no):
        'Generate an uid for the transaction\n        '
        self.seq_no = (self.seq_no + 1)
        return ((((((bank_name + '.') + str(account_no)) + '.') + str(self.id)) + '.') + str(self.seq_no))

    def send_request(self, request):
        " Request are of the form (('req_type', (UID, Acct. No, Amount*)))\n        "
        retries = 0
        while (retries <= self.num_retransmits):
            send_server = self.head
            if (request[0] == 'getBalance'):
                send_server = self.tail
            self.send_seq_no += 1
            self.output(self.client_name, 'SENT:', self.send_seq_no, ':', request, ' request to ', str(send_server))
            if (self.msg_loss_freq and ((self.send_seq_no % self.msg_loss_freq) == 0)):
                self.output(self.client_name, 'Simulate Msg loss of request', request)
            else:
                self._send(request, send_server)
            _st_label_417 = 0
            self._timer_start()
            while (_st_label_417 == 0):
                _st_label_417 += 1
                if self.server_reply:
                    self.server_reply = False
                    break
                    _st_label_417 += 1
                elif self._timer_expired:
                    retries += 1
                    if (retries > self.num_retransmits):
                        self.output(self.client_name, 'Giving Up on the servers')
                        break
                    self.output(self.client_name, 'Timed out waiting for a reply back from server, Retrying ', retries)
                    _st_label_417 += 1
                else:
                    super()._label('_st_label_417', block=True, timeout=self.client_time_out)
                    _st_label_417 -= 1
            else:
                if (_st_label_417 != 2):
                    continue
            if (_st_label_417 != 2):
                break

    def get_random_req(self, client_conf):
        x = random.uniform(0, 1)
        for (request_type, probability) in client_conf['prob']:
            if (x < probability):
                break
            x = (x - probability)
        account = random.randint((self.account_no - 100), (self.account_no + 100))
        sent_uid = self.generate_uid(self.bank_name, account)
        req = [sent_uid, account]
        if (not (request_type == 'getBalance')):
            req.append(random.randint(100, 1000))
        return (request_type, tuple(req))

    def _Client_handler_15(self, req, server):
        self.recv_seq_no += 1
        self.output(self.client_name, '#RECEIVED:', self.recv_seq_no, ': response ', req, ' from server ', server)
        if (self.msg_loss_freq and ((self.recv_seq_no % self.msg_loss_freq) == 0)):
            self.output(self.client_name, 'Simulate Msg loss of response', req)
        else:
            self.server_reply = True
    _Client_handler_15._labels = None
    _Client_handler_15._notlabels = None

    def _Client_handler_16(self, master, req):
        self.output(self.client_name, '#RECEIVED:', req, ' from Master ', master)
        self.head = req
        self.output(self.client_name, 'New head =', req)
    _Client_handler_16._labels = None
    _Client_handler_16._notlabels = None

    def _Client_handler_17(self, req, master):
        self.output(self.client_name, '#RECEIVED:', req, ' from Master ', master)
        self.tail = req
        self.output(self.client_name, 'New tail =', req)
    _Client_handler_17._labels = None
    _Client_handler_17._notlabels = None

class Master(da.DistProcess):

    def __init__(self, parent, initq, channel, props):
        super().__init__(parent, initq, channel, props)
        self._events.extend([da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_0', PatternExpr_36, sources=[PatternExpr_37], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_18]), da.pat.EventPattern(da.pat.ReceivedEvent, '_MasterReceivedEvent_1', PatternExpr_38, sources=[PatternExpr_39], destinations=None, timestamps=None, record_history=None, handlers=[self._Master_handler_19])])

    def setup(self, servers_set, clients_set, master_interval=5):
        self.servers_set = servers_set
        self.clients_set = clients_set
        self.master_interval = master_interval
        self.servers = list(self.servers_set)
        self.clients = list(self.clients_set)
        self.master_name = 'MASTER:'
        self.heartbeat_list = []
        self.init_heartbeat_list(self.servers)
        self.add_waitlist = []
        self.heartbeat_waitlist = []
        self.new_hb_list = []

    def main(self):
        self.output('### Master', self.master_name, self.id, ' firing up...')
        while True:
            super()._label('l', block=False)
            _st_label_478 = 0
            self._timer_start()
            while (_st_label_478 == 0):
                _st_label_478 += 1
                if False:
                    pass
                    _st_label_478 += 1
                elif self._timer_expired:
                    self.output('### Master', self.master_name, self.id, 'Processing Heartbeats.')
                    self.process_heartbeats()
                    _st_label_478 += 1
                else:
                    super()._label('l', block=True, timeout=self.master_interval)
                    _st_label_478 -= 1
            else:
                if (_st_label_478 != 2):
                    continue
            if (_st_label_478 != 2):
                break

    def process_heartbeats(self):
        head_failed = tail_failed = False
        last_index = (len(self.heartbeat_list) - 1)
        self.output(self.master_name, 'Hearbeat List = ', self.heartbeat_list)
        curr_tail = self.heartbeat_list[last_index][0]
        new_head = None
        new_tail = None
        for (i, elem) in enumerate(list(self.heartbeat_list)):
            if (new_head == None):
                if (not (elem[1] == 0)):
                    new_head = elem[0]
            if (not (elem[1] == 0)):
                new_tail = elem[0]
        for (i, elem) in enumerate(list(self.heartbeat_list)):
            if (elem[1] == 0):
                self.output(self.master_name, 'process_heartbeats:', elem[0], 'failed, informing servers')
                for server in self.servers:
                    if (not (server == elem[0])):
                        serv_fail = ('server_fail', (elem[0], new_head, new_tail))
                        self.output(self.master_name, 'SENT:', serv_fail, ' to', server)
                        self._send(serv_fail, server)
                if (i == 0):
                    head_failed = True
                elif (i == last_index):
                    tail_failed = True
                if (elem in self.heartbeat_list):
                    self.heartbeat_list.remove(elem)
                    self.servers.remove(elem[0])
        for entry in self.heartbeat_list:
            entry[1] = 0
        self.output(self.master_name, 'Hearbeat List of New Servers = ', self.new_hb_list)
        for (i, elem) in enumerate(list(self.new_hb_list)):
            if (elem[1] == 0):
                self.output(self.master_name, 'Curr Tail = ', curr_tail, 'New Tail = ', new_tail, type(new_tail))
                if (curr_tail == new_tail):
                    new_tail_fail_req = ('new_tail_fail', elem[0])
                    self.output(self.master_name, 'SENT:', new_tail_fail_req, ' to', curr_tail)
                    self._send(new_tail_fail_req, curr_tail)
                self.new_hb_list.remove(elem)
        for entry in self.new_hb_list:
            entry[1] = 0
        if head_failed:
            for client in self.clients:
                update_head = ('update_head', self.servers[0])
                self.output(self.master_name, 'SENT:', update_head, ' to', client)
                self._send(update_head, client)
        if tail_failed:
            if (not (self.new_hb_list == [])):
                purge_req = ('purge_req', 'Purge History')
                self.output(self.master_name, 'SENT:', purge_req, ' to', self.new_hb_list[0][0])
                self._send(purge_req, self.new_hb_list[0][0])
                sync_req = ('sync_new_tail', self.new_hb_list[0][0])
                self.output(self.master_name, 'SENT:', sync_req, ' to', self.servers[(- 1)])
                self._send(sync_req, self.servers[(- 1)])
            for client in self.clients:
                update_tail = ('update_tail', self.servers[(- 1)])
                self.output(self.master_name, 'SENT:', update_tail, ' to', client)
                self._send(update_tail, client)

    def init_heartbeat_list(self, servers):
        'Initialize the Heartbeat Counter List indexed by server id\n        '
        for elem in servers:
            self.heartbeat_list.append([elem, 0])

    def update_heartbeats(self, serv):
        'Increase the Count of Heartbeat Message from this server by 1\n        '
        for elem in self.heartbeat_list:
            if (elem[0] == serv):
                elem[1] += 1
                return
        self.output(self.master_name, 'self.new_hb_list = ', self.new_hb_list)
        if (not (serv in list(zip(*[([(None, None)] if (self.new_hb_list == []) else self.new_hb_list)][0]))[0])):
            self.output(self.master_name, 'NEW SERVER:', serv)
            self.new_hb_list.append([serv, 1])
            serv_list = ('servers_list', self.servers)
            self.output(self.master_name, 'SENT:', serv_list, ' to', serv)
            self._send(serv_list, serv)
            sync_req = ('sync_new_tail', serv)
            self.output(self.master_name, 'SENT:', sync_req, ' to', self.servers[(- 1)])
            self._send(sync_req, self.servers[(- 1)])
        else:
            for elem in self.new_hb_list:
                if (elem[0] == serv):
                    elem[1] += 1
                    return

    def _Master_handler_18(self, server):
        self.output(self.master_name, '#RECEIVED: Heartbeat message from server ', server)
        self.update_heartbeats(server)
    _Master_handler_18._labels = None
    _Master_handler_18._notlabels = None

    def _Master_handler_19(self, server):
        self.output(self.master_name, '#RECEIVED: New Tail Ready from server ', server)
        for servs in self.servers:
            add_new_tail = ('add_new_tail', server)
            self.output(self.master_name, 'SENT:', add_new_tail, ' to', servs)
            self._send(add_new_tail, servs)
        self.servers.append(server)
        self.heartbeat_list.append([server, 1])
        entries = [item for item in self.new_hb_list if (item[0] == server)]
        if entries:
            for entry in entries:
                self.new_hb_list.remove(entry)
        for client in self.clients:
            update_tail = ('update_tail', self.servers[(- 1)])
            self.output(self.master_name, 'SENT:', update_tail, ' to', client)
            self._send(update_tail, client)
    _Master_handler_19._labels = None
    _Master_handler_19._notlabels = None

def main():
    da.api.config(channel='fifo')
    for (name, conf) in server_conf.items():
        num_banks_in_chain = len(conf)
        print('Num banks in chain = ', num_banks_in_chain)
        servers = da.api.new(Server, num=num_banks_in_chain)
        num_clients = len(client_conf[name])
        clients = da.api.new(Client, num=num_clients)
        master = da.api.new(Master, num=1)
        for (i, c) in enumerate(clients):
            da.api.setup(c, [servers, name, i, master])
        for (i, s) in enumerate(servers):
            da.api.setup(s, [servers, name, i, master])
        print('Spawner Process: SERVER PROCESSES for', name, 'ARE')
        for (i, e) in enumerate(servers):
            print(i, e)
            print('Configuration:\n', server_conf[name][i])
        da.api.start(servers)
        print('\nSpawner Process: CLIENT PROCESSES for', name, 'ARE')
        for (i, e) in enumerate(clients):
            print(i, e)
            print('Configuration:\n', client_conf[name][i])
        da.api.start(clients)
        da.api.setup(master, [servers, clients, master_conf['master_interval']])
        da.api.start(master)
