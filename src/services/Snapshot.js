/**
 * Fieldfare: Backend framework for distributed networks
 *
 * Copyright 2021-2023 Adan Kvitschal
 * ISC LICENSE
 */

import { LocalService, Chunk, HostIdentifier, Utils, cryptoManager, logger} from '@fieldfare/core';

export {SnapshotService as implementation};

export const uuid = 'd2b38791-51af-4767-ad08-2f9f1425e90e';

export class SnapshotService extends LocalService {

    async store(remoteHost, stateMessage) {
        //1) Check if remoteHost belongs to the enviroment
        // if(await this.environment.belongs(remoteHost) === false) {
        //     throw Error('host does not belong to the environment');
        // }

        logger.debug('[STORE] Called by ' + remoteHost.id);
        //2) Check if provided state is valid
        Utils.validateParameters(stateMessage, ['service', 'data', 'signature', 'source']);
        Utils.validateParameters(stateMessage.data, ['id', 'ts', 'collections']);
        const timeDelta = Date.now() - stateMessage.data.ts;
        if(timeDelta > 1000 * 60 * 60 * 24) {   //a day
            logger.debug('[STORE] Rejected: state is too old');
            throw Error('state is too old');
        }
        const signatureIsValid = await cryptoManager.verifyMessage(stateMessage, remoteHost.pubKey);
        if(!signatureIsValid) {
            logger.debug('[STORE] Rejected: sigature is invalid');
            throw Error('signature is invalid');
        }

        //3) Check if provided state is newer than previous
        const hostStates = await this.collection.getElement('hostStates');
        if(!hostStates) {
            logger.debug('[STORE] Internal error: hostStates is null');
            throw Error('hostStates is null');
        }
        const keyChunk = Chunk.fromIdentifier(HostIdentifier.toChunkIdentifier(remoteHost.id));
        const valueChunk = await hostStates.get(keyChunk);
        let prevState = null;
        let stateIsNewer = false;
        let stateIsDifferent = false;
        if(valueChunk) {
            prevState = await valueChunk.expand(0);
            if(prevState.data.ts < stateMessage.data.ts) {
                stateIsNewer = true;
            }
                for (const uuid in stateMessage.data.collections) {
                        let status;
                        if(uuid in prevState.data.collections === false) {
                                status = ' [new]';
                                stateIsDifferent=true;
                        } else
                        if(prevState.data.collections[uuid] !== stateMessage.data.collections[uuid]) {
                    stateIsDifferent = true;
                                status = ' [change]';
                        } else {
                                status = ' [stale]';
                        }
                        logger.debug('[STORE-C'+gNumCalls+']' + uuid + ' => ' + stateMessage.data.collections[uuid] + status);
            }
        }
        if((stateIsNewer && stateIsDifferent) || !prevState) {
            const stateChunk = await Chunk.fromObject(stateMessage);
            stateChunk.ownerID = remoteHost.id;
            //4) Clone complete stat
            const startTime = Date.now();
            const numClonedChunks = await stateChunk.clone();
            const elapsedTime = Date.now() - startTime;
            logger.debug('SnapshotService.store() cloned ' + numClonedChunks + ' chunks in ' + elapsedTime + 'ms');
            //5) Store state as latest
            await hostStates.set(keyChunk, stateChunk);
            await this.collection.updateElement('hostStates', hostStates.descriptor);
            logger.debug('[STORE] state updated');
            return 'ok';
        }
        logger.debug('[STORE] state is stale');
        return 'state is stale';
    }

};