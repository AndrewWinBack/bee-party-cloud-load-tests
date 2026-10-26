import http from 'k6/http';
import {WebSocket} from 'k6/websockets';
import {SharedArray} from 'k6/data';
import {Counter,Rate,Trend,Gauge} from 'k6/metrics';
import exec from 'k6/execution';
import {check} from 'k6';
import {randomBytes,sha256} from 'k6/crypto';
const f=JSON.parse(open('../runtime/fixture.json'));
const records=new SharedArray('bee legal input trajectories',()=>JSON.parse(open('../runtime/sequence.json')));
const shard=Number(__ENV.SHARD_INDEX),N=f.players/4,workers=Math.min(80,N),batch=Math.ceil(N/workers),groups=Math.ceil(N/batch),planned=Math.ceil(N/5),runSeconds=f.duration+240;
const path=f.base+'/api/rooms/'+f.code,wsURL=path.replace(/^http/,'ws')+'/ws';
const record=(profile,seq)=>records[profile*f.stride+seq];
const gameErrors=new Rate('game_errors'),scoreMatches=new Rate('score_matches'),stateMatches=new Rate('restored_state_matches'),lateRejected=new Rate('deadline_rejected');
const joined=new Counter('players_joined'),readyCount=new Counter('players_ready'),finished=new Counter('players_finished'),ticks=new Counter('ticks_accepted'),updates=new Counter('updates_accepted');
const plannedCount=new Counter('planned_reconnections'),recovered=new Counter('planned_reconnections_recovered'),unexpected=new Counter('unexpected_disconnects'),timeoutCount=new Counter('ack_timeouts'),lateCount=new Counter('deadline_checks');
const scoreUpdates=new Counter('score_updates'),checkpoints=new Counter('checkpoints'),duplicates=new Counter('duplicate_retries');
const joinMs=new Trend('join_ms',true),readyMs=new Trend('ready_ms',true),ackMs=new Trend('score_ack_ms',true),endToEndMs=new Trend('update_confirm_ms',true),gapMs=new Trend('snapshot_gap_ms',true),timerLag=new Trend('generator_timer_lag_ms',true),recoveryMs=new Trend('recovery_ms',true),coverage=new Trend('simulation_coverage');
const connectionCount=new Gauge('server_connected_players');
const minTicks=Math.floor(N*(f.duration-0.2*5)*20*.85);
export const acceptanceThresholds={game_errors:['rate<0.01'],score_matches:['rate==1'],restored_state_matches:['rate==1'],deadline_rejected:['rate==1'],deadline_checks:['count=='+f.players],score_ack_ms:['p(95)<500','p(99)<1000'],update_confirm_ms:['p(95)<600','p(99)<1200'],join_ms:['p(95)<2000'],ready_ms:['p(95)<3000'],snapshot_gap_ms:['p(95)<2500'],generator_timer_lag_ms:['p(99)<200'],players_joined:['count=='+f.players],players_ready:['count=='+(f.players+Math.ceil(f.players/5))],players_finished:['count=='+f.players],planned_reconnections:['count=='+Math.ceil(f.players/5)],planned_reconnections_recovered:['count=='+Math.ceil(f.players/5)],unexpected_disconnects:['count==0'],ack_timeouts:['count==0'],ticks_accepted:['count>='+Math.floor(f.players*(f.duration-0.2*5)*20*.85)],simulation_coverage:['min>=0.85'],updates_accepted:['count>0'],score_updates:['count>0'],checkpoints:['count>0'],checks:['rate==1'],http_req_failed:['rate<0.01']};
export const options={
 scenarios:{players:{executor:'per-vu-iterations',vus:groups,iterations:1,maxDuration:runSeconds+'s',exec:'players'},...(shard===0?{controller:{executor:'per-vu-iterations',vus:1,iterations:1,maxDuration:runSeconds+'s',exec:'controller'},display:{executor:'per-vu-iterations',vus:1,iterations:1,maxDuration:runSeconds+'s',exec:'display'}}:{})},
 systemTags:['status','method','name','group','check','error','error_code','scenario'],summaryTrendStats:['avg','min','med','p(95)','p(99)','max']
};
const delay=ms=>new Promise(r=>setTimeout(r,ms));
const canonical=g=>JSON.stringify(g,(_k,v)=>typeof v==='number'?Math.round(v*1e6)/1e6:v);
function expectedScore(p){const i=Number(p.name.slice(5)),r=record(i%f.profiles,p.seq);return p.seq>0&&r&&r.s===p.score&&r.k===p.kills}
function required(value,label){if(!check(value,{[label]:v=>!!v}))throw new Error(label)}
export async function controller(){
 const key=__ENV.TEST_HOST_KEY;if(!key)throw new Error('Missing disposable staging host key');
 let socket,beat,retry,stop=false,hostReady=false,hostSnapshots=0,hostBoardPlayers=0,last=0;
 function connect(){if(stop)return;const c=new WebSocket(wsURL,[],{headers:{Cookie:__ENV.TEST_ADMIN_COOKIE,Origin:f.base}});socket=c;c.onopen=()=>c.send(JSON.stringify({type:'auth',role:'host',key,protocol:3}));c.onmessage=e=>{if(c!==socket||e.data==='pong')return;const m=JSON.parse(e.data);if(['ready','snapshot'].includes(m.type)){if(last)gapMs.add(Date.now()-last,{role:'host'});last=Date.now();hostReady=true;hostSnapshots++;hostBoardPlayers=m.players.length;connectionCount.add(m.connectedPlayers||0)}};c.onclose=e=>{if(stop||c!==socket)return;hostReady=false;unexpected.add(1,{role:'host',code:String(e.code)});console.warn(JSON.stringify({event:'unexpected_close',role:'host',code:e.code}));retry=setTimeout(connect,1000)}}
 async function host(body,suffix=''){const r=await http.asyncRequest(body?'POST':'GET',path+suffix,body?JSON.stringify(body):null,{headers:{Authorization:'Bearer '+key,'Content-Type':'application/json',Cookie:__ENV.TEST_ADMIN_COOKIE,'X-CSRF-Token':__ENV.TEST_ADMIN_CSRF,Origin:f.base},timeout:'20s',tags:{name:suffix?'host evidence':'host control'}});if(r.status!==200)throw new Error('Host HTTP '+r.status);return JSON.parse(r.body)}
 try{
  unexpected.add(0,{role:'host'});connect();beat=setInterval(()=>{if(socket?.readyState===1)socket.send('ping')},4000);
  const until=Date.now()+180000;let snapshot,all=false;
  while(Date.now()<until){snapshot=await host();if(hostReady&&snapshot.total===f.players&&snapshot.connectedPlayers===f.players){all=true;break}await delay(1000)}
  required(all,'800-or-requested players connected before start');
  const initialNetwork=await host(null,'/network');console.log(JSON.stringify({event:'network_initial',players:initialNetwork.players,uniqueSources:initialNetwork.uniqueSources,sources:initialNetwork.sources}));
  required(initialNetwork.players===f.players&&initialNetwork.uniqueSources===4&&initialNetwork.sources.length===4&&initialNetwork.sources.every(s=>s.players===f.players/4),'four independent sources with equal player counts recorded');
  snapshot=await host({action:'start',round:1});const room=snapshot.room;
  required(room.endsAt-room.startsAt===f.duration*1000,'exact requested duration');console.log(JSON.stringify({event:'match_started',room:f.code,players:f.players,startsAt:room.startsAt,endsAt:room.endsAt}));
  while(Date.now()<room.endsAt+2500)await delay(500);
  const final=await host(),ranks=final.players;required(final.room.status==='finished'&&ranks.length===f.players,'all players present at automatic settlement');
  required(ranks.every(expectedScore),'every final score and kill count matches replay');
  const rankOK=ranks.every((p,i)=>!i?p.rank===1:((ranks[i-1].score>p.score||ranks[i-1].score===p.score&&ranks[i-1].kills>=p.kills)&&p.rank===(ranks[i-1].score===p.score&&ranks[i-1].kills===p.kills?ranks[i-1].rank:i+1)));
  required(rankOK,'complete ranking and tied ranks correct');required(hostSnapshots>0&&hostBoardPlayers===f.players,'host receives complete leaderboard');
  const history=await host(null,'/history?round=1');required(history.players.length===ranks.length&&history.players.every((p,i)=>['id','name','score','kills','seq','alive','rank'].every(k=>p[k]===ranks[i][k])),'persisted history equals final leaderboard');
  const network=await host(null,'/network');console.log(JSON.stringify({event:'network_final',players:network.players,uniqueSources:network.uniqueSources,sources:network.sources,events:network.events,commitTiming:network.commitTiming,boot:network.boot,diagnosticCount:network.diagnostics.length}));
  console.log(JSON.stringify({event:'cloud_verification',ok:true,room:f.code,players:f.players,verifiedMatchSeconds:(room.endsAt-room.startsAt)/1000,hostSnapshots,hostBoardPlayers,allScoresVerified:true,rankingVerified:rankOK,historyVerified:true,totalTicks:ranks.reduce((n,p)=>n+p.seq,0),minTicks:Math.min(...ranks.map(p=>p.seq)),maxTicks:Math.max(...ranks.map(p=>p.seq)),scoreSum:ranks.reduce((n,p)=>n+p.score,0),kills:ranks.reduce((n,p)=>n+p.kills,0)}));
 }catch(e){console.error(JSON.stringify({event:'cloud_control_failed',message:e.message}));exec.test.abort('Cloud controller validation failed')}
 finally{stop=true;clearInterval(beat);clearTimeout(retry);socket?.close(1000)}
}
export function display(){
 let socket,beat,retry,stop=false,last=0,snapshots=0;
 const deadline=setTimeout(()=>{stop=true;clearInterval(beat);clearTimeout(retry);socket?.close(1000)},runSeconds*1000-5000);
 function connect(){if(stop)return;const c=new WebSocket(wsURL);socket=c;c.onopen=()=>{c.send(JSON.stringify({type:'auth',role:'display',protocol:3}));clearInterval(beat);beat=setInterval(()=>{if(c.readyState===1)c.send('ping')},4000)};c.onmessage=e=>{if(c!==socket||e.data==='pong')return;const m=JSON.parse(e.data);if(['ready','snapshot'].includes(m.type)){if(last)gapMs.add(Date.now()-last,{role:'display'});last=Date.now();snapshots++;if(m.room.status==='playing')scoreMatches.add(m.spectators.every(p=>p.seq===0||expectedScore(p)));if(m.room.status==='finished'){check(m,{'display receives correct final top three':d=>d.spectators.length===Math.min(3,f.players)&&d.spectators.every(expectedScore)});console.log(JSON.stringify({event:'display_complete',snapshots}));stop=true;clearTimeout(deadline);clearInterval(beat);c.close(1000)}}};c.onclose=e=>{if(stop||c!==socket)return;unexpected.add(1,{role:'display',code:String(e.code)});console.warn(JSON.stringify({event:'unexpected_close',role:'display',code:e.code}));clearInterval(beat);retry=setTimeout(connect,1000)}}
 unexpected.add(0,{role:'display'});connect();
}
export async function players(){
 const group=exec.scenario.iterationInTest;
 gameErrors.add(false);scoreMatches.add(true);stateMatches.add(true);unexpected.add(0,{role:'player'});timeoutCount.add(0);duplicates.add(0);checkpoints.add(0);scoreUpdates.add(0);
 if(f.joinRampSeconds)await delay(group*batch/N*f.joinRampSeconds*1000);
 for(let j=0;j<batch;j++){
  const localIndex=group*batch+j;if(localIndex>=N)break;const index=shard*N+localIndex;const name='LOAD-'+String(index).padStart(5,'0');
  const joinKey=Array.from(new Uint8Array(randomBytes(32)),v=>v.toString(16).padStart(2,'0')).join('');
  const start=Date.now();let response;
  for(let a=0;a<3;a++){response=await http.asyncRequest('POST',path,JSON.stringify({action:'join',name,joinKey}),{headers:{'Content-Type':'application/json'},timeout:'8s',tags:{name:'join'}});if(response.status===200||response.status>0&&response.status<500&&![408,429].includes(response.status)||a===2)break;await delay(a===0?500:1500)}
  joinMs.add(Date.now()-start);if(response.status!==200){gameErrors.add(true);console.error(JSON.stringify({event:'join_failed',index,status:response.status}));continue}joined.add(1);startPlayer(index,response.json().key);
 }
}
function startPlayer(index,key){
 const profile=index%f.profiles;
 let room,ws,beat,timer,retry,done=false,ready=false,roundStart=0,offset=0,seq=0,localSeq=0,localParts=[],queue=[],pending=null,retryAfter=0,plannedDone=false,recoverAt=0,finalSeen=false,lastSnapshot=0,lastMessage=0,errorsLogged=0,requestSequence=0,rttMean=0,rttDeviation=0,requestTimeout=5000,failures=0;
 const deadline=setTimeout(()=>{done=true;clearInterval(timer);clearInterval(beat);clearTimeout(retry);ws?.close(1000)},runSeconds*1000-6000);
 function errorLog(event,detail){if(errorsLogged++<4)console.warn(JSON.stringify({event,index,...detail}))}
 function connect(){
  if(done)return;const opened=Date.now(),c=new WebSocket(wsURL);ws=c;let intentional=false;ready=false;lastMessage=opened;
  c.onopen=()=>c.send(JSON.stringify({type:'auth',role:'player',key,protocol:3}));
  c.onmessage=e=>{
   if(c!==ws||done)return;const now=Date.now();lastMessage=now;if(e.data==='pong')return;const m=JSON.parse(e.data);
   if(m.type==='ready'){
    required(m.protocol===3,'compact protocol 3 negotiated');
    offset=m.serverNow-now;ready=true;seq=m.me.game.seq;localSeq=seq;localParts=[];queue=[];pending=null;retryAfter=0;lastSnapshot=0;readyMs.add(now-opened);readyCount.add(1);
    const expected=record(profile,seq);scoreMatches.add(expected?.s===m.me.game.score&&expected?.k===m.me.game.kills);stateMatches.add(expected?.h===sha256(canonical(m.me.game),'hex'));
    if(recoverAt){recovered.add(1);recoveryMs.add(now-recoverAt);recoverAt=0}
    clearInterval(timer);clearInterval(beat);let due=now+50;
    timer=setInterval(()=>{const now=Date.now();timerLag.add(Math.max(0,now-due));due=now+50;play()},50);
    beat=setInterval(()=>{if(ws!==c)return;if(nowSince(lastMessage)>10000){intentional=true;unexpected.add(1,{role:'player',code:'watchdog'});c.close();retry=setTimeout(connect,500);return}if(c.readyState===1&&nowSince(lastMessage)>=4000)c.send('ping')},4000);
   }
   if(['ready','snapshot','progress'].includes(m.type)){
    if(m.room)room=m.room;roundStart=room.startsAt;
    if(room.status==='playing'&&now+offset>=room.startsAt){if(lastSnapshot)gapMs.add(now-lastSnapshot,{role:'player'});lastSnapshot=now}else lastSnapshot=0;
    if(room.status==='finished'&&!finalSeen){
     finalSeen=true;finished.add(1);const expected=record(profile,m.me.seq);scoreMatches.add(expected?.s===m.me.score&&expected?.k===m.me.kills&&m.me.seq===seq);
     const playableSeconds=f.duration-(plannedDone?5:0);coverage.add(seq/(playableSeconds*20));
     c.send(JSON.stringify({type:'checkpoint',requestId:'late',round:1,fromSeq:seq,seq:seq+10,input:'0000000000'}));
     setTimeout(()=>{done=true;clearInterval(timer);clearInterval(beat);clearTimeout(deadline);c.close(1000)},1500);
    }
   }
   if(m.type==='ack'&&pending&&m.requestId===pending.id){
    const expected=record(profile,m.game.seq);scoreMatches.add(expected?.s===m.game.score&&expected?.k===m.game.kills&&m.game.seq===pending.seq);ackMs.add(now-pending.at);const sample=Math.max(1,now-pending.at);if(!rttMean){rttMean=sample;rttDeviation=sample/2}else{rttDeviation=.75*rttDeviation+.25*Math.abs(rttMean-sample);rttMean=.875*rttMean+.125*sample}requestTimeout=Math.max(1500,Math.min(5000,rttMean+4*rttDeviation+500));endToEndMs.add(now-pending.enqueuedAt);gameErrors.add(false);ticks.add(m.game.seq-seq);updates.add(1);if(pending.type==='score')scoreUpdates.add(1);else checkpoints.add(1);if(m.duplicate)duplicates.add(1);seq=m.game.seq;queue.shift();pending=null;failures=0;retryAfter=now+120;
   }
   if(m.type==='error'){
    if(m.requestId==='late'){lateCount.add(1);lateRejected.add(m.status===409);return}
    if(pending&&m.requestId===pending.id){
     if(m.status===409&&m.error==='本場不在計分時間'&&now+offset>=room.endsAt-1000){pending=null;return}
     gameErrors.add(true);errorLog('game_error',{status:m.status,error:m.error,seq});pending=null;failures++;retryAfter=now+Math.min(1500,250*2**(failures-1));if(m.status===400||m.status===409||failures>=3)c.send(JSON.stringify({type:'sync'}));
    }
   }
  };
  c.onerror=e=>{if(c!==ws||done)return;gameErrors.add(true);errorLog('socket_error',{message:String(e.error||e.message||'socket error'),seq})};
  c.onclose=e=>{if(c!==ws)return;ready=false;clearInterval(timer);clearInterval(beat);if(done||intentional)return;unexpected.add(1,{role:'player',code:String(e.code)});errorLog('unexpected_close',{code:e.code,reason:e.reason,eventType:e.type,eventKeys:Object.keys(e),socketState:c.readyState,age:Date.now()-opened,lastMessageAge:Date.now()-lastMessage,seq,phase:room?.status});retry=setTimeout(connect,500)};
  function play(){
   const now=Date.now(),serverNow=now+offset;if(done||!ready||!room||!['playing','countdown'].includes(room.status)||serverNow<room.startsAt||serverNow>=room.endsAt||c.readyState!==1)return;
   if(!plannedDone&&index%5===0&&serverNow>=room.startsAt+f.duration*500){plannedDone=true;intentional=true;recoverAt=now;plannedCount.add(1);c.close(1000,'planned five-second interruption');retry=setTimeout(connect,f.reconnectDelayMs);return}
   if(pending&&now-pending.at>requestTimeout){timeoutCount.add(1);gameErrors.add(true);errorLog('ack_timeout',{seq,pending:pending.seq});pending=null;requestTimeout=Math.min(5000,requestTimeout*1.5);retryAfter=now+250}
   pump();if(localSeq-seq>=60||localSeq+1>Math.floor((serverNow-room.startsAt)/50))return;
   const next=record(profile,localSeq+1);if(!next)return;localSeq++;localParts.push([next.a]);
   if(localParts.length===10){const from=localSeq-10,prev=record(profile,from),type=next.s>prev.s?'score':'checkpoint';queue.push({type,fromSeq:from,seq:localSeq,pieces:localParts,enqueuedAt:now});localParts=[];pump()}
  }
  function pump(){
   const now=Date.now();if(done||!ready||pending||!queue.length||retryAfter>now||!room||now+offset>=room.endsAt||c.readyState!==1)return;
   let item=queue[0];
   if(!item.attempted)while(queue.length>1&&!queue[1].attempted&&item.seq===queue[1].fromSeq&&item.pieces.length+queue[1].pieces.length<=30){const next=queue[1];item={...item,type:item.type==='score'||next.type==='score'?'score':'checkpoint',seq:next.seq,pieces:[...item.pieces,...next.pieces]};queue.splice(0,2,item)}
   item.attempted=true;const id=(++requestSequence).toString(36);pending={...item,id,at:now};c.send(JSON.stringify({type:item.type,requestId:id,round:1,fromSeq:item.fromSeq,seq:item.seq,input:item.pieces.map(p=>p[0]==='left'?'1':p[0]==='right'?'2':'0').join('')}));
  }
 }
 function nowSince(at){return Date.now()-at}
 connect();
}
export function handleSummary(data){return {'runtime/summary.json':JSON.stringify({...data,acceptanceThresholds},null,2)}}
