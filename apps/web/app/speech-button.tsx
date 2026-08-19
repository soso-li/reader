"use client";

import { useEffect, useRef, useState } from "react";
import { Pause, Volume2 } from "lucide-react";

export default function SpeechButton({ text }: { text: string }) {
  const [playing, setPlaying] = useState(false);
  const utterance = useRef<SpeechSynthesisUtterance | null>(null);

  useEffect(() => {
    window.speechSynthesis?.cancel();
    utterance.current = null;
    setPlaying(false);
    return () => window.speechSynthesis?.cancel();
  }, [text]);

  function toggle() {
    if (!text.trim() || !window.speechSynthesis) return;
    if (playing) {
      window.speechSynthesis.pause();
      setPlaying(false);
      return;
    }
    if (utterance.current && window.speechSynthesis.paused) {
      window.speechSynthesis.resume();
      setPlaying(true);
      return;
    }
    window.speechSynthesis.cancel();
    const next = new SpeechSynthesisUtterance(text.replace(/\s+/g, " ").trim());
    next.onend = () => setPlaying(false);
    next.onerror = () => setPlaying(false);
    utterance.current = next;
    window.speechSynthesis.speak(next);
    setPlaying(true);
  }

  return (
    <button className="icon" type="button" title={playing ? "暂停朗读" : "朗读"} aria-label={playing ? "暂停朗读" : "朗读"} onClick={toggle}>
      {playing ? <Pause size={17} /> : <Volume2 size={17} />}
    </button>
  );
}
