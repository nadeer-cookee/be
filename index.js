const express = require("express");
const mongoose = require("mongoose");
const http = require("http");
const socketIo = require("socket.io");
const cors = require("cors");
const redis = require("redis");
const { promisify } = require("util");
const { randomDate, randomTime } = require("./utils");
const rateLimit = require("express-rate-limit");
const Queue = require("bull");
const winston = require("winston");
const mongoosePaginate = require("mongoose-paginate-v2");

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
  },
});

// Redis client setup
const redisClient = redis.createClient({
  url: "redis://localhost:6379",
});

redisClient.on("error", (err) => console.log("Redis Client Error", err));
redisClient.on("connect", () => console.log("Connected to Redis"));

// Connect to Redis
(async () => {
  await redisClient.connect();
})();

const getAsync = redisClient.get.bind(redisClient);
const setAsync = redisClient.set.bind(redisClient);

// Express middleware
app.use(cors());
app.use(express.json());

// Rate limiting
const apiLimiter = rateLimit({
  windowMs: 15 * 60 * 1000, // 15 minutes
  max: 100, // limit each IP to 100 requests per windowMs
});
app.use("/api/", apiLimiter);

// MongoDB connection
mongoose.connect("mongodb://localhost:27017/slot-booking", {
  useNewUrlParser: true,
  useUnifiedTopology: true,
});

// Schemas
const SlotSchema = new mongoose.Schema({
  date: Date,
  time: String,
  isBooked: Boolean,
});
SlotSchema.plugin(mongoosePaginate);

const BookingSchema = new mongoose.Schema({
  slot: { type: mongoose.Schema.Types.ObjectId, ref: "Slot" },
  userEmail: String,
  bookingDate: Date,
});

const Slot = mongoose.model("Slot", SlotSchema);
const Booking = mongoose.model("Booking", BookingSchema);

// Logging setup
const logger = winston.createLogger({
  level: "info",
  format: winston.format.json(),
  defaultMeta: { service: "slot-booking-service" },
  transports: [
    new winston.transports.File({ filename: "error.log", level: "error" }),
    new winston.transports.File({ filename: "combined.log" }),
  ],
});

// Job queue setup
const bookingQueue = new Queue("booking-queue", "redis://127.0.0.1:6379");

// Helper function to cache slots in Redis
async function cacheSlots(key, slots) {
  await setAsync(key, JSON.stringify(slots), {
    EX: 600, // Cache for 10 minutes
  });
}

// Routes
app.get("/api/slots", async (req, res) => {
  try {
    const { date, time, isBooked, page = 1, limit = 20 } = req.query;
    let filter = {};

    if (date) filter.date = new Date(date);
    if (time) filter.time = time;
    if (isBooked !== undefined) filter.isBooked = isBooked === "true";

    const cacheKey = `slots:${JSON.stringify(filter)}:${page}:${limit}`;

    // Try to get slots from Redis cache
    const cachedSlots = await getAsync(cacheKey);
    if (cachedSlots) {
      return res.json(JSON.parse(cachedSlots));
    }

    // If not in cache, get from database and cache the result
    const options = {
      page: parseInt(page, 10),
      limit: parseInt(limit, 10),
    };

    const result = await Slot.paginate(filter, options);
    await cacheSlots(cacheKey, result);
    res.json(result);
  } catch (error) {
    logger.error("Error fetching slots:", error);
    res
      .status(500)
      .json({ message: "Error fetching slots", error: error.message });
  }
});

app.post("/api/book", async (req, res) => {
  const { slotId, userEmail } = req.body;
  try {
    // Validate input
    if (!slotId) {
      throw new Error("Missing required fields");
    }

    // Check if slot is already booked
    const slot = await Slot.findById(slotId);
    if (slot.isBooked) {
      throw new Error("Slot already booked");
    }

    // Add job to queue instead of processing immediately
    const job = await bookingQueue.add(
      {
        slotId,
        userEmail,
      },
      {
        priority: 1,
        attempts: 3,
        backoff: {
          type: "exponential",
          delay: 2000,
        },
      }
    );

    res.json({ message: "Booking request received", jobId: job.id });
  } catch (error) {
    logger.error("Error queueing booking:", error);
    res
      .status(500)
      .json({ message: "Booking request failed", error: error.message });
  }
});

// Job processor
bookingQueue.process(async (job) => {
  const { slotId, userEmail } = job.data;

  // Simulate progress
  await job.progress(25);

  try {
    const result = await bookSlot(slotId, userEmail);
    await job.progress(100);
    return result;
  } catch (error) {
    throw new Error(`Booking failed: ${error.message}`);
  }
});

// Error handler
bookingQueue.on("failed", (job, err) => {
  console.error(`Job ${job.id} failed with error ${err.message}`);
  // Here you might want to notify the user that their booking failed
});

// API endpoint to check job status
app.get("/api/booking-status/:jobId", async (req, res) => {
  const { jobId } = req.params;

  try {
    const job = await bookingQueue.getJob(jobId);
    if (!job) {
      return res.status(404).json({ message: "Job not found" });
    }

    const state = await job.getState();
    const progress = job._progress;
    const result = job.returnvalue;

    res.json({ jobId, state, progress, result });
  } catch (error) {
    res
      .status(500)
      .json({ message: "Error fetching job status", error: error.message });
  }
});

// Booking function with transactions
async function bookSlot(slotId, userEmail) {
  const session = await mongoose.startSession();
  session.startTransaction();

  try {
    const slot = await Slot.findById(slotId).session(session);
    if (slot.isBooked) {
      throw new Error("Slot already booked");
    }

    slot.isBooked = true;
    await slot.save({ session });

    const booking = new Booking({
      slot: slotId,
      userEmail,
      bookingDate: new Date(),
    });
    await booking.save({ session });

    await session.commitTransaction();
    session.endSession();

    // Update cache
    const slots = await Slot.find();
    await cacheSlots("slots", slots);

    // Emit the update to all connected clients
    io.emit("slotUpdate", { slotId, isBooked: true });

    return booking;
  } catch (error) {
    await session.abortTransaction();
    session.endSession();
    throw error;
  }
}

app.post("/api/generate-slots", async (req, res) => {
  try {
    const startDate = new Date("2024-01-01");
    const endDate = new Date("2024-12-31");

    const generatedSlots = [];

    for (let i = 0; i < 400000; i++) {
      generatedSlots.push({
        date: randomDate(startDate, endDate),
        time: randomTime(),
        isBooked: Math.random() < 0.3, // 30% chance of being booked
      });
    }

    const result = await Slot.insertMany(generatedSlots);

    res.status(201).json({
      message: "Slots generated and inserted successfully",
      insertedCount: result.insertedCount,
    });
  } catch (error) {
    logger.error("Error generating and inserting slots:", error);
    res.status(500).json({ error: "Internal server error" });
  }
});

// Socket.IO connection handling
io.on("connection", (socket) => {
  console.log("New client connected");

  socket.on("disconnect", () => {
    console.log("Client disconnected");
  });
});

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error(err.stack);
  res.status(500).send("Something broke!");
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => console.log(`Server running on port ${PORT}`));

// Graceful shutdown
process.on("SIGINT", async () => {
  await redisClient.quit();
  await bookingQueue.close();
  mongoose.connection.close();
  process.exit(0);
});
