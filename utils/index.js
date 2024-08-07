exports.randomDate = (start, end) => {
    return new Date(
      start.getTime() + Math.random() * (end.getTime() - start.getTime())
    );
  };
  
  exports.randomTime = () => {
    const hours = Math.floor(Math.random() * 24)
      .toString()
      .padStart(2, "0");
    const minutes = Math.floor(Math.random() * 60)
      .toString()
      .padStart(2, "0");
    return `${hours}:${minutes}`;
  };