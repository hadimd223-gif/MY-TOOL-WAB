// Updated UID Checker Logic with robust name and avatar parsing
async function checkPlayerUID() {
    const uid = document.getElementById('game-player-id').value.trim();
    if(!uid) return Swal.fire('Error', 'Please Enter Player UID First!', 'warning');

    const modal = document.getElementById('uid-modal');
    const body = document.getElementById('uid-modal-body');
    modal.classList.remove('hidden');
    body.innerHTML = `<div class="text-center py-8 text-white"><i class="fas fa-spinner fa-spin text-xl"></i><p class="mt-2 text-xs">Fetching Full Account Details...</p></div>`;

    try {
        const res = await fetch(`https://auraffxserverinfocheckerapi.onrender.com/api/player?uid=${uid}`);
        const data = await res.json();
        
        if(data) {
            // Safe extraction for all possible data properties from API response
            const info = data.basicInfo || data.accountInfo || data.playerInfo || data;
            
            const name = info.nickname || info.name || data.nickname || data.name || "Unknown Player";
            const level = info.level || data.level || "N/A";
            const likes = info.likes || data.likes || "N/A";
            const region = info.region || data.region || "BD";
            const signature = info.signature || data.signature || "No signature available";
            
            // Avatar & Banner image mapping fix
            const avatar = info.avatar || info.avatarUrl || info.profilePic || data.avatar || "https://placehold.co/100";
            const banner = info.banner || info.bannerUrl || data.banner || "";

            body.innerHTML = `
                <div class="bg-black/90 p-3.5 rounded-xl border border-white/20 space-y-3">
                    <div class="flex items-center gap-3 border-b border-white/10 pb-3">
                        <img src="${avatar}" class="w-14 h-14 rounded-xl object-cover border border-white/30 bg-white/10" onerror="this.src='https://placehold.co/100'">
                        <div>
                            <h4 class="font-bold text-white text-sm">${name}</h4>
                            <p class="text-[10px] text-white/70">UID: ${uid} (${region})</p>
                            <span class="inline-block mt-1 px-2 py-0.5 bg-white/10 rounded text-[9px] text-white font-bold">Level: ${level}</span>
                        </div>
                    </div>

                    <div class="grid grid-cols-2 gap-2 text-[11px]">
                        <div class="bg-white/5 p-2 rounded-lg border border-white/10">Likes: <span class="font-bold text-white">${likes}</span></div>
                        <div class="bg-white/5 p-2 rounded-lg border border-white/10">Region: <span class="font-bold text-white">${region}</span></div>
                    </div>

                    <div class="bg-white/5 p-2.5 rounded-lg border border-white/10 text-[10px]">
                        <span class="text-white/60 block mb-0.5 font-bold">Signature / Bio:</span> 
                        <p class="text-white font-medium">${signature}</p>
                    </div>
                </div>
            `;
        } else {
            body.innerHTML = `<div class="text-center py-6 text-red-400">Player ID Not Found or Invalid UID!</div>`;
        }
    } catch(err) {
        body.innerHTML = `<div class="text-center py-6 text-red-400">Failed to fetch details. Server Error.</div>`;
    }
}

function closeUidModal() {
    document.getElementById('uid-modal').classList.add('hidden');
}
