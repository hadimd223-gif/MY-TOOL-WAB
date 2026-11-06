// Firebase Cloud Messaging service worker — keep this file in the SAME folder as index.html (site root).
importScripts("https://www.gstatic.com/firebasejs/9.22.1/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/9.22.1/firebase-messaging-compat.js");
firebase.initializeApp({
  apiKey: "AIzaSyAJWDIhsO4nAnBvpVZ0FP3kIlHFEU2Ysq4",
  authDomain: "m-h-x-wab-7abdf.firebaseapp.com",
  projectId: "m-h-x-wab-7abdf",
  messagingSenderId: "570748526283",
  appId: "1:570748526283:web:3c786486aa3f838188c54e"
});
// Messages sent by the server carry a "notification" payload, so the browser shows them automatically
// (also when the site is closed) and opens the link on tap.
firebase.messaging();
self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));
