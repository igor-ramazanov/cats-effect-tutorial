import java.net._
import java.nio.ByteBuffer
import java.nio.channels.{
  AsynchronousChannelGroup,
  AsynchronousServerSocketChannel,
  AsynchronousSocketChannel,
  CompletionHandler
}
import java.util.concurrent.{ExecutorService, Executors}

import cats.effect._
import cats.effect.concurrent.MVar
import cats.effect.implicits._
import cats.implicits._

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

/**
  * Asynchronous TCP NIO2 echo server.
  * For the better understanding and easier debugging all thread-pools are single-threaded.
  *
  * Even with single-threaded pools it's able to concurrently process many users.
  *
  * Pay attention to the thread name in logs after `Async[F].async` operations.
  *
  * '''Try to check your understanding by guessing thread names in putStrLn logs before actual running!'''
  *
  * After running connect by `telnet localhost 5432`
  *
  * "STOP" - stops server
  * empty line - closes connection
  * any other string - returns echo
  *
  * @see [[https://gist.github.com/djspiewak/46b543800958cf61af6efa8e072bfd5c]]
  * @see [[https://typelevel.org/cats-effect/tutorial/tutorial.html]]
  */
object TcpEchoServer extends IOApp {

  /**
    * Event-loop pool, handles incoming connections and schedules actual work on other dedicated pools.
    * Threads in this type of pool normally should have a maximum priority.
    * Normally should be a fixed-size thread pool with the size of 1 or 2.
    */
  val handlerPool: ExecutionContextExecutor = ExecutionContext
    .fromExecutor(
      Executors
        .newSingleThreadExecutor(
          (r: Runnable) => createDaemonThread(r, "event-loop-pool")
        )
    )

  /**
    * IO-pool for executing blocking and long computations.
    * Normally should be an unbounded cached thread pool.
    */
  val ioPool: ExecutorService =
    Executors.newSingleThreadExecutor(createDaemonThread(_, "io-pool"))

  /**
    * CPU-pool for executing CPU-heavy tasks.
    * Normally should be a fixed-size thread pool with
    * {{{Runtime.getRuntime.availableProcessors()}}}
    * or
    * {{{Runtime.getRuntime.availableProcessors() * 2}}} number of threads.
    */
  val cpuPool: ExecutionContext = ExecutionContext.fromExecutor(
    Executors.newSingleThreadExecutor(createDaemonThread(_, "cpu-pool"))
  )

  /**
    * Timer pool, schedules tasks on the [[handlerPool]] pool.
    * Threads priority and count are questionable and not clear for me.
    * Intuition and rationality tells me it should be a 1-2 fixed-size thread pool with the highest priority.
    */
  override implicit protected def timer: Timer[IO] =
    IO.timer(
      handlerPool,
      Executors.newSingleThreadScheduledExecutor(
        (r: Runnable) => createDaemonThread(r, "scheduler-pool")
      )
    )

  /**
    * "Saves" the [[handlerPool]] pool into its context.
    * [[ContextShift.shift]] operation forces asynchronous boundary
    * by rescheduling following IO-suspended operations to the stored [[handlerPool]].
    */
  override implicit protected def contextShift: ContextShift[IO] =
    IO.contextShift(handlerPool)

  def createDaemonThread(r: Runnable, name: String): Thread = {
    val t = new Thread(r, name)
    t.setDaemon(true)
    t
  }

  override def run(args: List[String]): IO[ExitCode] = {

    def close[F[_]: Sync](socket: AsynchronousServerSocketChannel): F[Unit] =
      Sync[F].delay(socket.close()).handleErrorWith(_ => Sync[F].unit)

    IO.delay(
        AsynchronousServerSocketChannel
          .open(AsynchronousChannelGroup.withThreadPool(ioPool))
          .bind(new InetSocketAddress("localhost", 5432))
      )
      .bracket { asynchronousServerSocketChannel =>
        server[IO](asynchronousServerSocketChannel) >>= (
          code =>
            putStrLn[IO](s"server finishing, $currentThreadName") >>
              code.pure[IO]
        )
      } { asynchronousServerSocketChannel =>
        close[IO](asynchronousServerSocketChannel) >>
          putStrLn[IO](s"server closed, $currentThreadName")
      }
  }

  def server[F[_]: Concurrent: ContextShift](
    asynchronousServerSocketChannel: AsynchronousServerSocketChannel
  ): F[ExitCode] = {
    for {
      stopFlag <- MVar[F].empty[Unit]
      serverFiber <- serve(asynchronousServerSocketChannel, stopFlag).start
      _ <- stopFlag.read
      _ <- serverFiber.cancel.start
    } yield ExitCode.Success
  }

  def serve[F[_]: Concurrent: ContextShift](
    asynchronousServerSocketChannel: AsynchronousServerSocketChannel,
    stopFlag: MVar[F, Unit]
  ): F[Unit] = {

    def close(socket: AsynchronousSocketChannel): F[Unit] =
      Sync[F].delay(socket.close()).handleErrorWith(_ => Sync[F].unit)

    for {
      _ <- putStrLn(s"before .accept(), $currentThreadName")
      errorOrSocket <- Async[F]
        .async[AsynchronousSocketChannel] { cb =>
          asynchronousServerSocketChannel.accept(
            ".accept()",
            completionHandler(cb)
          )
        }
        .attempt
      _ <- putStrLn(s"after .accept(), $currentThreadName")
      _ <- ContextShift[F].shift
      _ <- putStrLn(s"shifted, $currentThreadName")
      _ <- errorOrSocket.fold(
        e => putStrLn(s"failed to create socket: $e, ${e.getMessage}"),
        socket =>
          for {
            fiber <- echoProtocol(socket, stopFlag).start
            _ <- (fiber.join >> close(socket)).start
          } yield ()
      )
      _ <- putStrLn(s"after socketFiber.start, $currentThreadName")
      _ <- serve(asynchronousServerSocketChannel, stopFlag)
    } yield ()
  }

  def echoProtocol[F[_]: Async: ContextShift](
    clientSocket: AsynchronousSocketChannel,
    stopFlag: MVar[F, Unit]
  ): F[Unit] = {
    val buffer = ByteBuffer.allocateDirect(1024)
    //buffer.array() fails for unknown reason
    val secondBuffer = Array.ofDim[Byte](1024)

    def loop(): F[Unit] =
      for {
        _ <- putStrLn(s"before .read(), $currentThreadName")
        count <- Async[F].async[Integer] { cb =>
          clientSocket.read(buffer, ".read()", completionHandler(cb))
        }
        _ <- putStrLn(s"after .read(), $currentThreadName")
        _ <- ContextShift[F].shift
        _ <- putStrLn(s"shifted, $currentThreadName")
        _ <- if (count != -1) {
          for {
            _ <- putStrLn(s"before response calculation, $currentThreadName")
            line <- ContextShift[F].evalOn(cpuPool)(
              Sync[F]
                .delay {
                  //assume we have a CPU-heavy task here for calculating an answer
                  println(s"response calculation, $currentThreadName")
                  buffer.flip()
                  buffer.get(secondBuffer, 0, count)
                  buffer.position(0)
                  new String(secondBuffer.slice(0, count)).trim
                }
            )
            _ <- putStrLn(s"after response calculation, $currentThreadName")
            _ <- line match {
              case "STOP" => stopFlag.put(())
              case ""     => putStrLn(s"closing socket, $currentThreadName")
              case _ =>
                for {
                  _ <- putStrLn(s"before .write(), $currentThreadName")
                  _ <- Async[F].async[Integer] { cb =>
                    clientSocket.write(
                      buffer,
                      ".write()",
                      completionHandler(cb)
                    )
                  }
                  _ <- putStrLn(s"after .write(), $currentThreadName")
                  _ <- ContextShift[F].shift
                  _ <- putStrLn(s"shifted, $currentThreadName")
                  _ <- Sync[F].delay(buffer.clear())
                  _ <- loop()
                } yield ()
            }
          } yield ()
        } else putStrLn(s"socket closed, $currentThreadName")
      } yield ()

    loop()
  }

  def completionHandler[A](
    cb: Either[Throwable, A] => Unit
  ): CompletionHandler[A, String] = new CompletionHandler[A, String] {
    override def completed(result: A, attachment: String): Unit = {
      println(s"inside async $attachment, $currentThreadName")
      cb(Right(result))
    }

    override def failed(exc: Throwable, attachment: String): Unit =
      cb(Left(exc))
  }

  def putStrLn[F[_]: Sync](s: String): F[Unit] = Sync[F].delay(println(s))

  def currentThreadName: String = Thread.currentThread().getName
}
