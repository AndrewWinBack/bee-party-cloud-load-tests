import fs from 'node:fs';import path from 'node:path';import {execFileSync} from 'node:child_process';import assert from 'node:assert/strict';
import {createDecipheriv,createCipheriv,randomBytes} from 'node:crypto';import {createGunzip,gzipSync} from 'node:zlib';import {createInterface} from 'node:readline';
const repo=process.env.GITHUB_REPOSITORY,run=process.env.SOURCE_RUN_ID,token=process.env.GH_TOKEN,key=Buffer.from(process.env.BUNDLE_KEY,'hex');
assert.equal(repo,'AndrewWinBack/bee-party-cloud-load-tests');assert.match(run||'',/^\d+$/);assert.equal(key.length,32);
execFileSync('gh',['run','download',run,'--repo',repo,'--dir','relay'],{stdio:'inherit',timeout:180000});
const api=async(p,body)=>{const r=await fetch('https://api.github.com/repos/'+repo+p,{method:body?'POST':'GET',headers:{Authorization:'Bearer '+token,Accept:'application/vnd.github+json','Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});if(!r.ok)throw Error('GitHub API '+r.status);return r.json()};
const entries=[];
for(const name of fs.readdirSync('relay')){
 const dir=path.join('relay',name),data=JSON.parse(fs.readFileSync(path.join(dir,'results.enc.json')));assert.deepEqual(Object.keys(data).sort(),['data','iv','tag']);
 const decipher=createDecipheriv('aes-256-gcm',key,Buffer.from(data.iv,'hex'));decipher.setAuthTag(Buffer.from(data.tag,'hex'));
 fs.writeFileSync(dir+'/results.tar.gz',Buffer.concat([decipher.update(Buffer.from(data.data,'base64')),decipher.final()]));execFileSync('tar',['-xzf',dir+'/results.tar.gz','-C',dir]);
 const histograms={};let pointCount=0;
 const lines=createInterface({input:fs.createReadStream(dir+'/points.gz').pipe(createGunzip()),crlfDelay:Infinity});
 for await(const line of lines){const p=JSON.parse(line);if(p.type!=='Point')continue;pointCount++;const values=histograms[p.metric]??={};values[p.data.value]=(values[p.data.value]||0)+1;}
 const compact={format:'exact-value-counts-v1',pointCount,histograms,summary:JSON.parse(fs.readFileSync(dir+'/summary.json')),execution:JSON.parse(fs.readFileSync(dir+'/execution.json')),log:fs.readFileSync(dir+'/run.log','utf8')};
 if(fs.existsSync(dir+'/transport.json'))compact.transport=JSON.parse(fs.readFileSync(dir+'/transport.json'));
 const iv=randomBytes(12),cipher=createCipheriv('aes-256-gcm',key,iv),encrypted=Buffer.concat([cipher.update(gzipSync(JSON.stringify(compact))),cipher.final()]);
 const content=Buffer.from(JSON.stringify({iv:iv.toString('hex'),tag:cipher.getAuthTag().toString('hex'),data:encrypted.toString('base64')}));
 const blob=await api('/git/blobs',{encoding:'base64',content:content.toString('base64')});entries.push({path:name+'.enc.json',mode:'100644',type:'blob',sha:blob.sha});
 console.log(JSON.stringify({shard:compact.execution.shard,points:pointCount,compactBytes:content.length}));
}
assert.equal(entries.length,4);
const tree=await api('/git/trees',{tree:entries}),commit=await api('/git/commits',{message:'Encrypted exact-value histograms for run '+run,tree:tree.sha,parents:[]});
await api('/git/refs',{ref:'refs/heads/compact-'+run,sha:commit.sha});
console.log(JSON.stringify({run,files:4,branch:'compact-'+run,encryptedOnly:true}));
