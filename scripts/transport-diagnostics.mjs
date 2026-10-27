import {spawn} from 'node:child_process';import {createHash,randomBytes} from 'node:crypto';import {lookup} from 'node:dns/promises';
// Passive TCP reset metadata only: no payload, host addresses or credentials
// are retained. An independent flow hash permits deduplication within this run.
export function parseReset(line,salt){
 const m=line.match(/^(\d+\.\d+).*?IP (\d+\.\d+\.\d+\.\d+)\.(\d+) > (\d+\.\d+\.\d+\.\d+)\.(\d+): Flags \[([^\]]*R[^\]]*)\]/);if(!m)return null;
 return {at:new Date(Number(m[1])*1000).toISOString(),direction:Number(m[3])===443?'fromRemote443':Number(m[5])===443?'toRemote443':'other',flags:m[6],flow:createHash('sha256').update(salt+':'+[m[2]+':'+m[3],m[4]+':'+m[5]].sort().join('|')).digest('hex').slice(0,20)};
}
export async function startTransportDiagnostics(base){
 const events=[],salt=randomBytes(32).toString('hex'),result={available:false,events,packetsCaptured:null,packetsDropped:null,limitReached:false};let child,buffer='';
 try{const records=await lookup(new URL(base).hostname,{all:true,family:4});const addresses=[...new Set(records.map(r=>r.address))];if(!addresses.length||addresses.some(a=>!/^\d+\.\d+\.\d+\.\d+$/.test(a)))throw Error('resolve failed');
 child=spawn('sudo',['-n','tcpdump','-n','-l','-tt','-i','any','tcp port 443 and (tcp[tcpflags] & tcp-rst != 0) and ('+addresses.map(a=>'host '+a).join(' or ')+')'],{stdio:['ignore','pipe','pipe']});
 child.on('error',()=>{result.available=false});
 child.stderr.on('data',b=>{const s=b.toString();if(s.includes('listening on'))result.available=true;const count=s.match(/(\d+) packets captured/),drop=s.match(/(\d+) packets dropped by kernel/);if(count)result.packetsCaptured=Number(count[1]);if(drop)result.packetsDropped=Number(drop[1])});
 child.stdout.on('data',b=>{buffer+=b.toString();const lines=buffer.split('\n');buffer=lines.pop();for(const line of lines){const event=parseReset(line,salt);if(event){if(events.length<2000)events.push(event);else result.limitReached=true}}});
 }catch{result.available=false}
 return async()=>{if(child&&child.exitCode===null){child.kill('SIGINT');await Promise.race([new Promise(r=>child.once('exit',r)),new Promise(r=>setTimeout(r,3000))]);if(child.exitCode===null)child.kill('SIGKILL')}return result};
}
